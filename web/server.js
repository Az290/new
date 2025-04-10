const express = require('express');
const net = require('net');
const bodyParser = require('body-parser');
const path = require('path');

const app = express();
const PORT = 3001;

// Middleware
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, 'public')));

// CORS middleware
app.use(function(req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE');
    next();
});

// Lưu trữ lịch sử
let queryHistory = [];

// Biến để lưu trữ database hiện tại
let currentDatabase = null;

// Quản lý kết nối TCP liên tục
let persistentSocket = null;
let isConnecting = false;
let connectionQueue = [];
let activeRequests = new Map(); // Map để theo dõi các yêu cầu đang hoạt động

// Hàm để đảm bảo có kết nối TCP
function ensureConnection(callback) {
    // Nếu đã có kết nối và socket vẫn mở
    if (persistentSocket && !persistentSocket.destroyed) {
        callback(null, persistentSocket);
        return;
    }
    
    // Nếu đang trong quá trình kết nối, thêm vào hàng đợi
    if (isConnecting) {
        connectionQueue.push(callback);
        return;
    }
    
    isConnecting = true;
    console.log('[TCP] Creating new persistent connection to DuckSQL server');
    
    const socket = new net.Socket();
    
    // Thiết lập timeout
    socket.setTimeout(10000); // 10 giây timeout
    
    socket.connect(8080, '127.0.0.1', function() {
        console.log('[TCP] Connected to DuckSQL server');
        persistentSocket = socket;
        isConnecting = false;
        
        // Gọi callback hiện tại
        callback(null, socket);
        
        // Xử lý hàng đợi
        while (connectionQueue.length > 0) {
            const queuedCallback = connectionQueue.shift();
            queuedCallback(null, socket);
        }
    });
    
    socket.on('error', function(err) {
        console.error('[TCP] Socket error:', err.message);
        persistentSocket = null;
        isConnecting = false;
        
        // Thông báo lỗi cho callback hiện tại
        callback(err);
        
        // Thông báo lỗi cho tất cả các callback trong hàng đợi
        while (connectionQueue.length > 0) {
            const queuedCallback = connectionQueue.shift();
            queuedCallback(err);
        }
    });
    
    socket.on('timeout', function() {
        console.log('[TCP] Connection timeout');
        socket.destroy();
        persistentSocket = null;
        isConnecting = false;
        
        const timeoutError = new Error('Connection timeout');
        
        // Thông báo lỗi cho callback hiện tại
        callback(timeoutError);
        
        // Thông báo lỗi cho tất cả các callback trong hàng đợi
        while (connectionQueue.length > 0) {
            const queuedCallback = connectionQueue.shift();
            queuedCallback(timeoutError);
        }
    });
    
    socket.on('close', function() {
        console.log('[TCP] Connection closed');
        persistentSocket = null;
        
        // Xử lý các yêu cầu đang hoạt động
        for (const [requestId, callback] of activeRequests.entries()) {
            callback(new Error('Connection closed'));
            activeRequests.delete(requestId);
        }
    });
}

// Hàm gửi truy vấn qua kết nối TCP liên tục
function sendQuery(query, callback) {
    const requestId = Date.now() + Math.random().toString(36).substring(2, 15);
    activeRequests.set(requestId, callback);
    
    ensureConnection(function(err, socket) {
        if (err) {
            console.error('[QUERY] Connection error:', err.message);
            activeRequests.delete(requestId);
            return callback(err);
        }
        
        console.log(`[QUERY] Sending query: ${query}`);
        
        // Chuẩn hóa truy vấn
        let normalizedQuery = query.trim();
        if (normalizedQuery.startsWith('\uFEFF')) {
            normalizedQuery = normalizedQuery.slice(1);
        }
        normalizedQuery = normalizedQuery.replace(/[^\x20-\x7E]/g, '');
        
        // Biến để theo dõi xem callback đã được gọi chưa
        let callbackCalled = false;
        
        // Đặt handler cho dữ liệu nhận về
        const responseHandler = function(data) {
            let responseData = data.toString('utf8');
            console.log('[QUERY] Received data:', responseData);
            
            try {
                // Kiểm tra xem có phải JSON hợp lệ không
                const parsedData = JSON.parse(responseData);
                
                // Nếu là JSON hợp lệ, xóa request từ activeRequests và gọi callback
                if (!callbackCalled) {
                    callbackCalled = true;
                    activeRequests.delete(requestId);
                    
                    // Kiểm tra nếu đây là phản hồi cho lệnh USE
                    const useMatch = query.match(/^use\s+(\w+)$/i);
                    if (useMatch && !parsedData.error) {
                        currentDatabase = useMatch[1];
                        console.log(`[QUERY] Database set to: ${currentDatabase}`);
                    }
                    
                    // Kiểm tra nếu có thông báo lỗi về database
                    if (parsedData.error && (
                        parsedData.error.includes("database not selected") || 
                        parsedData.error.includes("database doesn't exist"))) {
                        currentDatabase = null;
                        console.log('[QUERY] Reset current database due to error in response');
                    }
                    
                    socket.removeListener('data', responseHandler);
                    callback(null, parsedData);
                }
            } catch (e) {
                // Nếu không phải JSON hợp lệ, có thể là do dữ liệu chưa nhận đủ
                // Chúng ta sẽ chờ sự kiện data tiếp theo
            }
        };
        
        // Thiết lập timeout cho request
        const requestTimeout = setTimeout(function() {
            if (!callbackCalled) {
                callbackCalled = true;
                activeRequests.delete(requestId);
                socket.removeListener('data', responseHandler);
                callback(new Error('Request timeout'));
            }
        }, 15000); // 15 giây timeout cho request
        
        socket.on('data', responseHandler);
        
        // Xử lý lỗi trong quá trình nhận dữ liệu
        const errorHandler = function(err) {
            if (!callbackCalled) {
                callbackCalled = true;
                activeRequests.delete(requestId);
                clearTimeout(requestTimeout);
                socket.removeListener('data', responseHandler);
                socket.removeListener('error', errorHandler);
                callback(err);
            }
        };
        
        socket.once('error', errorHandler);
        
        // Gửi truy vấn
        socket.write(normalizedQuery, 'utf8', function(err) {
            if (err && !callbackCalled) {
                callbackCalled = true;
                activeRequests.delete(requestId);
                clearTimeout(requestTimeout);
                socket.removeListener('data', responseHandler);
                socket.removeListener('error', errorHandler);
                callback(err);
            }
        });
    });
}

// Endpoint để kiểm tra trạng thái kết nối
app.get('/check-connection', function(req, res) {
    let responseSet = false;
    
    ensureConnection(function(err, socket) {
        if (responseSet) return; // Tránh gửi phản hồi nhiều lần
        
        responseSet = true;
        if (err) {
            console.error('[CHECK-CONNECTION] Error:', err.message);
            res.json({ connected: false });
        } else {
            console.log('[CHECK-CONNECTION] Successfully connected to server C');
            res.json({ connected: true });
        }
    });
    
    // Timeout cho endpoint này
    setTimeout(function() {
        if (!responseSet) {
            responseSet = true;
            res.json({ connected: false, error: 'Request timeout' });
        }
    }, 5000);
});

// API endpoint để xử lý truy vấn
app.post('/query', function(req, res) {
    try {
        const query = req.body.query && req.body.query.trim();
        console.log('[SERVER] Received query:', query);

        if (!query) {
            return res.status(400).json({ error: 'Query cannot be empty' });
        }

        let responseSet = false;
        
        sendQuery(query, function(error, result) {
            if (responseSet) return; // Tránh gửi phản hồi nhiều lần
            
            responseSet = true;
            
            if (error) {
                console.error('[SERVER ERROR]', error.message);
                return res.status(500).json({ 
                    status: 'error',
                    error: error.message 
                });
            }

            // Lưu vào lịch sử
            queryHistory.unshift({
                query: query,
                response: result,
                timestamp: new Date().toISOString()
            });

            // Giới hạn lịch sử
            if (queryHistory.length > 50) {
                queryHistory.pop();
            }

            // Thêm thông tin về database hiện tại vào response
            res.json({
                status: 'success',
                result: result,
                currentDatabase: currentDatabase,
                timestamp: new Date().toLocaleString()
            });
        });
        
        // Timeout cho endpoint này
        setTimeout(function() {
            if (!responseSet) {
                responseSet = true;
                res.status(504).json({ 
                    status: 'error',
                    error: 'Request timeout' 
                });
            }
        }, 20000); // 20 giây timeout

    } catch (error) {
        console.error('[SERVER ERROR]', error.message);
        res.status(500).json({ 
            status: 'error',
            error: error.message 
        });
    }
});

// Thêm endpoint để lấy database hiện tại
app.get('/current-database', function(req, res) {
    res.json({ 
        database: currentDatabase,
        connected: (persistentSocket && !persistentSocket.destroyed)
    });
});

// Lấy lịch sử
app.get('/history', function(req, res) {
    res.json(queryHistory.slice(0, 20));
});

// Xóa lịch sử
app.delete('/history', function(req, res) {
    queryHistory = [];
    res.json({ status: 'success' });
});

// Khởi động server
const server = app.listen(PORT, function() {
    console.log(`Web server running on http://localhost:${PORT}`);
    console.log(`Ensure DuckSQL server is running on port 8080`);
});

// Xử lý khi ứng dụng đóng để đóng kết nối TCP
process.on('exit', function() {
    if (persistentSocket && !persistentSocket.destroyed) {
        persistentSocket.destroy();
    }
});

process.on('SIGINT', function() {
    console.log('\nClosing TCP connection and exiting...');
    if (persistentSocket && !persistentSocket.destroyed) {
        persistentSocket.destroy();
    }
    server.close();
    process.exit();
});
