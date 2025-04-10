const express = require('express');
const net = require('net');
const bodyParser = require('body-parser');
const path = require('path');

const app = express();
const PORT = 3000;

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

// Endpoint để kiểm tra trạng thái kết nối đến server C
app.get('/check-connection', function(req, res) {
    const client = new net.Socket();
    client.setTimeout(2000); // Timeout 2 giây

    client.connect(8080, '127.0.0.1', function() {
        console.log('[CHECK-CONNECTION] Successfully connected to server C');
        res.json({ connected: true });
        client.destroy();
    });

    client.on('error', function(err) {
        console.error('[CHECK-CONNECTION] Error:', err.message);
        res.json({ connected: false });
        client.destroy();
    });

    client.on('timeout', function() {
        console.log('[CHECK-CONNECTION] Connection timeout');
        res.json({ connected: false });
        client.destroy();
    });
});

// API endpoint để xử lý truy vấn
app.post('/query', function(req, res) {
    try {
        const query = req.body.query && req.body.query.trim();
        console.log('[SERVER] Received query:', query);

        if (!query) {
            return res.status(400).json({ error: 'Query cannot be empty' });
        }

        sendToDuckSQL(query, function(error, result) {
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

            res.json({
                status: 'success',
                result: result,
                timestamp: new Date().toLocaleString()
            });
        });

    } catch (error) {
        console.error('[SERVER ERROR]', error.message);
        res.status(500).json({ 
            status: 'error',
            error: error.message 
        });
    }
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

// Hàm kết nối tới DuckSQL server (callback style)
function sendToDuckSQL(query, callback) {
    const client = new net.Socket();
    const SERVER_PORT = 8080;
    const SERVER_IP = '127.0.0.1';
    const TIMEOUT = 10000; // 10 giây timeout

    let responseData = '';
    let isResolved = false;

    const timeout = setTimeout(function() {
        client.destroy();
        if (!isResolved) {
            isResolved = true;
            callback(new Error('Connection timeout'));
        }
    }, TIMEOUT);

    // Chuẩn hóa truy vấn: đảm bảo viết thường, loại bỏ ký tự thừa, và thêm newline
    let normalizedQuery = query.toLowerCase().trim();
    // Loại bỏ BOM (nếu có)
    if (normalizedQuery.startsWith('\uFEFF')) {
        normalizedQuery = normalizedQuery.slice(1);
    }
    // Loại bỏ các ký tự không mong muốn (chỉ giữ ký tự ASCII cơ bản)
    normalizedQuery = normalizedQuery.replace(/[^\x20-\x7E]/g, '');
    console.log(`[SEND-TO-DUCKSQL] Attempting to connect to ${SERVER_IP}:${SERVER_PORT} with query: ${normalizedQuery}`);

    client.connect(SERVER_PORT, SERVER_IP, function() {
        console.log('[SEND-TO-DUCKSQL] Connected to server');
        // Gửi dữ liệu với encoding UTF-8
        client.write(normalizedQuery, 'utf8');
    });

    client.on('data', function(data) {
        responseData += data.toString('utf8');
        console.log('[SEND-TO-DUCKSQL] Received data:', data.toString('utf8'));

        // Gọi callback ngay sau khi nhận dữ liệu, không chờ sự kiện close
        if (!isResolved) {
            isResolved = true;
            clearTimeout(timeout);
            try {
                const parsedData = JSON.parse(responseData);
                callback(null, parsedData);
            } catch (err) {
                callback(new Error('Invalid JSON response from server: ' + responseData));
            }
            // Đóng socket chủ động từ phía client
            client.destroy();
        }
    });

    client.on('close', function() {
        console.log('[SEND-TO-DUCKSQL] Connection closed');
        // Không gọi callback ở đây nữa vì đã gọi trong sự kiện data
    });

    client.on('error', function(err) {
        clearTimeout(timeout);
        if (!isResolved) {
            isResolved = true;
            console.error('[SEND-TO-DUCKSQL] Error:', err.message);
            callback(err);
        }
    });
}

// Khởi động server
app.listen(PORT, function() {
    console.log(`Web server running on http://localhost:${PORT}`);
    console.log(`Ensure DuckSQL server is running on port 8080`);
});
