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
let activeRequests = new Map(); // Map để theo dọi các yêu cầu đang hoạt động
let lastActivity = Date.now();
let nextRequestId = 1;

// Hàm để tạo kết nối TCP mới
function createConnection(callback) {
    if (isConnecting) {
        connectionQueue.push(callback);
        return;
    }
    
    isConnecting = true;
    console.log('[TCP] Creating new persistent connection to DuckSQL server');
    
    const socket = new net.Socket();
    
    // Vô hiệu hóa timeout của socket
    socket.setTimeout(0);
    
    socket.connect(8080, '127.0.0.1', function() {
        console.log('[TCP] Connected to DuckSQL server');
        persistentSocket = socket;
        isConnecting = false;
        lastActivity = Date.now();
        
        // Gọi callback hiện tại
        callback(null, socket);
        
        // Xử lý hàng đợi
        while (connectionQueue.length > 0) {
            const queuedCallback = connectionQueue.shift();
            queuedCallback(null, socket);
        }
    });
    
    // Lắng nghe dữ liệu để cập nhật lastActivity và xử lý các phản hồi
    socket.on('data', function(data) {
        lastActivity = Date.now();
        let responseData = data.toString('utf8');
        console.log('[TCP] Received data:', responseData);
        
        try {
            // Kiểm tra xem có phải JSON hợp lệ không
            const parsedData = JSON.parse(responseData);
            
            // Tìm request đang hoạt động đầu tiên để xử lý phản hồi này
            if (activeRequests.size > 0) {
                const [requestId, callback] = [...activeRequests.entries()][0];
                activeRequests.delete(requestId);
                
                // Kiểm tra nếu đây là phản hồi cho lệnh USE
                for (const [id, req] of Object.entries(pendingQueries)) {
                    const useMatch = req.query.match(/^use\s+(\w+)$/i);
                    if (useMatch && !parsedData.error) {
                        currentDatabase = useMatch[1];
                        console.log(`[TCP] Database set to: ${currentDatabase}`);
                    }
                    
                    // Kiểm tra nếu có thông báo lỗi về database
                    if (parsedData.error && (
                        parsedData.error.includes("database not selected") || 
                        parsedData.error.includes("database doesn't exist"))) {
                        currentDatabase = null;
                        console.log('[TCP] Reset current database due to error in response');
                    }
                    
                    // Xóa truy vấn khỏi danh sách đang chờ
                    delete pendingQueries[id];
                    break;
                }
                
                callback(null, parsedData);
            }
        } catch (e) {
            // Nếu không phải JSON hợp lệ, bỏ qua
            console.log('[TCP] Received non-JSON data:', responseData);
        }
    });
    
    socket.on('error', function(err) {
        console.error('[TCP] Socket error:', err.message);
        
        // KHÔNG đánh dấu socket đã bị đóng
        // persistentSocket = null;
        
        // Thông báo lỗi cho callback hiện tại, nhưng không đóng kết nối
        callback(err);
        
        // Thông báo lỗi cho tất cả các callback trong hàng đợi
        while (connectionQueue.length > 0) {
            const queuedCallback = connectionQueue.shift();
            queuedCallback(err);
        }
    });
    
    socket.on('close', function() {
        console.log('[TCP] Connection closed unexpectedly, will reconnect on next query');
        
        // Đánh dấu socket đã bị đóng để có thể tạo lại khi cần
        persistentSocket = null;
        
        // Thông báo cho tất cả các yêu cầu đang hoạt động
        for (const [requestId, callback] of activeRequests.entries()) {
            callback(new Error('Connection closed unexpectedly'));
            activeRequests.delete(requestId);
        }
    });
}

// Hàm để đảm bảo có kết nối TCP
function ensureConnection(callback) {
    // Nếu đã có kết nối và socket vẫn mở
    if (persistentSocket && !persistentSocket.destroyed) {
        callback(null, persistentSocket);
        return;
    }
    
    // Tạo kết nối mới
    createConnection(callback);
}

// Theo dõi các truy vấn đang chờ xử lý
let pendingQueries = {};

// Hàm gửi truy vấn qua kết nối TCP liên tục
function sendQuery(query, callback) {
    const requestId = nextRequestId++;
    activeRequests.set(requestId, callback);
    
    // Lưu truy vấn vào danh sách đang chờ
    pendingQueries[requestId] = {
        query: query,
        timestamp: Date.now()
    };
    
    ensureConnection(function(err, socket) {
        if (err) {
            console.error('[QUERY] Connection error:', err.message);
            activeRequests.delete(requestId);
            delete pendingQueries[requestId];
            
            // Vẫn thông báo lỗi nhưng không đóng kết nối
            return callback(err);
        }
        
        console.log(`[QUERY] Sending query: ${query}`);
        lastActivity = Date.now();
        
        // Chuẩn hóa truy vấn
        let normalizedQuery = query.trim();
        if (normalizedQuery.startsWith('\uFEFF')) {
            normalizedQuery = normalizedQuery.slice(1);
        }
        normalizedQuery = normalizedQuery.replace(/[^\x20-\x7E]/g, '');
        
        // Tạo một timeout riêng cho truy vấn này (không phải cho socket)
        const queryTimeout = setTimeout(function() {
            if (activeRequests.has(requestId)) {
                console.log(`[QUERY] Query timeout for request ${requestId}, but keeping connection open`);
                
                // Xóa khỏi danh sách đang chờ
                activeRequests.delete(requestId);
                delete pendingQueries[requestId];
                
                // Vẫn thông báo lỗi nhưng không đóng kết nối
                callback(new Error('Query timeout'));
            }
        }, 10000); // 10 giây timeout cho truy vấn
        
        // Lưu timeout vào danh sách đang chờ
        pendingQueries[requestId].timeout = queryTimeout;
        
        // Gửi truy vấn
        socket.write(normalizedQuery, 'utf8', function(err) {
            if (err) {
                clearTimeout(queryTimeout);
                activeRequests.delete(requestId);
                delete pendingQueries[requestId];
                
                console.log(`[QUERY] Error sending query: ${err.message}`);
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
        
        // Nếu kết nối hiện tại đã quá cũ (30 phút), tạo mới (nhưng không đóng cũ)
        const inactivityTime = Date.now() - lastActivity;
        if (inactivityTime > 30 * 60 * 1000) {
            console.log('[SERVER] Connection inactive for 30 minutes, will create new connection');
            persistentSocket = null; // Chỉ đánh dấu để tạo mới, không đóng
        }
        
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

// Ping endpoint để giữ kết nối TCP hoạt động
app.get('/ping', function(req, res) {
    lastActivity = Date.now(); // Cập nhật thời gian hoạt động cuối cùng
    
    if (persistentSocket && !persistentSocket.destroyed) {
        res.json({ status: 'ok', connected: true, database: currentDatabase });
    } else {
        ensureConnection(function(err, socket) {
            if (err) {
                res.json({ status: 'error', connected: false, error: err.message });
            } else {
                res.json({ status: 'ok', connected: true, database: currentDatabase });
            }
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
    
    // Tạo kết nối ban đầu
    ensureConnection(function(err, socket) {
        if (err) {
            console.error('[STARTUP] Failed to establish initial connection:', err.message);
        } else {
            console.log('[STARTUP] Initial connection established successfully');
        }
    });
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
