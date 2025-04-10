class DuckSQLClient {
    constructor() {
        this.initElements();
        this.bindEvents();
        this.checkServerStatus();
        this.loadHistory();
    }

    initElements() {
        this.queryInput = document.getElementById('queryInput');
        this.executeBtn = document.getElementById('executeBtn');
        this.clearBtn = document.getElementById('clearBtn');
        this.resultOutput = document.getElementById('resultOutput');
        this.resultMeta = document.getElementById('resultMeta');
        this.historyList = document.getElementById('historyList');
        this.historyCount = document.getElementById('historyCount');
        this.clearHistoryBtn = document.getElementById('clearHistoryBtn');
        this.serverStatus = document.getElementById('serverStatus');
    }

    bindEvents() {
        this.executeBtn.addEventListener('click', () => this.executeQuery());
        this.clearBtn.addEventListener('click', () => this.clearQuery());
        this.clearHistoryBtn.addEventListener('click', () => this.clearHistory());
        
        this.queryInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && e.ctrlKey) {
                this.executeQuery();
            }
        });
    }

    async checkServerStatus() {
        try {
            const response = await fetch('/check-connection', {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' }
            });
            
            const data = await response.json();
            const statusDot = this.serverStatus.querySelector('.status-dot');
            const statusText = this.serverStatus.querySelector('span:last-child');

            if (data.connected) {
                statusDot.classList.add('connected');
                statusText.textContent = 'Đã kết nối';
                statusText.style.color = 'green';
            } else {
                statusDot.classList.remove('connected');
                statusText.textContent = 'Chưa kết nối';
                statusText.style.color = 'red';
            }
        } catch (error) {
            console.error('Server check failed:', error);
            const statusDot = this.serverStatus.querySelector('.status-dot');
            const statusText = this.serverStatus.querySelector('span:last-child');
            statusDot.classList.remove('connected');
            statusText.textContent = 'Chưa kết nối';
            statusText.style.color = 'red';
        }
    }

    async executeQuery() {
        const query = this.queryInput.value.trim();
        if (!query) {
            this.showResult('Vui lòng nhập truy vấn SQL', 'error');
            return;
        }

        this.showResult('Đang thực thi...', 'info');

        try {
            const startTime = Date.now();
            const response = await fetch('/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query })
            });

            const data = await response.json();
            const executionTime = Date.now() - startTime;

            if (data.status === 'error') {
                this.showResult(`Lỗi: ${data.error}`, 'error');
            } else {
                this.showResult(this.formatResult(JSON.stringify(data.result)), 'success');
                this.resultMeta.innerHTML = `
                    <span>Thực thi thành công trong ${executionTime}ms</span>
                    <span>•</span>
                    <span>${new Date().toLocaleTimeString()}</span>
                `;
                this.loadHistory();
            }
        } catch (error) {
            console.error('Execute error:', error);
            this.showResult(`Lỗi kết nối: Không thể kết nối đến server C (127.0.0.1:8080). Vui lòng kiểm tra xem server có đang chạy không.`, 'error');
        }
    }

    formatResult(result) {
        try {
            // Try to parse as JSON
            const json = JSON.parse(result);
            return this.syntaxHighlight(JSON.stringify(json, null, 2));
        } catch {
            // Return as plain text if not JSON
            return result;
        }
    }

    syntaxHighlight(json) {
        json = json.replace(/&/g, '&').replace(/</g, '<').replace(/>/g, '>');
        return json.replace(
            /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
            (match) => {
                let cls = 'json-number';
                if (/^"/.test(match)) {
                    if (/:$/.test(match)) {
                        cls = 'json-key';
                    } else {
                        cls = 'json-string';
                    }
                } else if (/true|false/.test(match)) {
                    cls = 'json-boolean';
                } else if (/null/.test(match)) {
                    cls = 'json-null';
                }
                return `<span class="${cls}">${match}</span>`;
            }
        );
    }

    showResult(content, type = 'info') {
        this.resultOutput.innerHTML = content;
        this.resultOutput.className = type;
    }

    clearQuery() {
        this.queryInput.value = '';
        this.queryInput.focus();
    }

    async loadHistory() {
        try {
            const response = await fetch('/history');
            const history = await response.json();
            this.renderHistory(history);
        } catch (error) {
            console.error('Load history error:', error);
        }
    }

    renderHistory(history) {
        this.historyList.innerHTML = '';
        this.historyCount.textContent = `(${history.length})`;

        if (history.length === 0) {
            this.historyList.innerHTML = '<p class="empty-history">Chưa có lịch sử truy vấn</p>';
            return;
        }

        history.forEach((item, index) => {
            const historyItem = document.createElement('div');
            historyItem.className = 'history-item';
            historyItem.innerHTML = `
                <div class="history-query">${item.query.split('\n')[0]}</div>
                <div class="history-time">${new Date(item.timestamp).toLocaleString()}</div>
                <div class="history-snippet">${JSON.stringify(item.response).substring(0, 100)}${JSON.stringify(item.response).length > 100 ? '...' : ''}</div>
            `;
            
            historyItem.addEventListener('click', () => {
                this.queryInput.value = item.query;
                this.showResult(this.formatResult(JSON.stringify(item.response)), 'success');
                this.resultMeta.innerHTML = `
                    <span>Tải lại từ lịch sử</span>
                    <span>•</span>
                    <span>${new Date(item.timestamp).toLocaleTimeString()}</span>
                `;
            });

            this.historyList.appendChild(historyItem);
        });
    }

    async clearHistory() {
        if (!confirm('Bạn có chắc muốn xóa toàn bộ lịch sử?')) return;
        
        try {
            await fetch('/history', { method: 'DELETE' });
            this.loadHistory();
        } catch (error) {
            console.error('Clear history error:', error);
            alert('Xóa lịch sử thất bại');
        }
    }
}

// Khởi tạo ứng dụng khi DOM tải xong
document.addEventListener('DOMContentLoaded', () => {
    new DuckSQLClient();
});
