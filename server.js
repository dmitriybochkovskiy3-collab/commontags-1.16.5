const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Хранилище комнат: roomName -> Set<WebSocket>
const rooms = new Map();

wss.on('connection', (ws) => {
    ws.currentRoom = null;
    ws.isAlive = true;

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', (data) => {
        let msg;
        try {
            msg = JSON.parse(data);
        } catch (e) {
            return;
        }

        const type = msg.type;
        const room = msg.room;

        if (!type) return;

        // Обработка PING -> быстрый PONG без бродкаста
        if (type === 'PING') {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'PONG' }));
            }
            return;
        }

        // Регистрация в комнате
        if (room) {
            if (ws.currentRoom !== room) {
                leaveRoom(ws);
                joinRoom(ws, room);
            }
        }

        if (!ws.currentRoom) return;

        // Бродкаст сообщения всем участникам комнаты, кроме отправителя
        const clients = rooms.get(ws.currentRoom);
        if (clients) {
            const payload = JSON.stringify(msg);
            for (const client of clients) {
                if (client !== ws && client.readyState === WebSocket.OPEN) {
                    client.send(payload);
                }
            }
        }
    });

    ws.on('close', () => {
        leaveRoom(ws);
    });

    ws.on('error', () => {
        leaveRoom(ws);
    });
});

function joinRoom(ws, room) {
    ws.currentRoom = room;
    if (!rooms.has(room)) {
        rooms.set(room, new Set());
    }
    rooms.get(room).add(ws);
    console.log(`[+] Client joined room '${room}'. Members: ${rooms.get(room).size}`);
}

function leaveRoom(ws) {
    if (ws.currentRoom && rooms.has(ws.currentRoom)) {
        const clientSet = rooms.get(ws.currentRoom);
        clientSet.delete(ws);
        console.log(`[-] Client left room '${ws.currentRoom}'. Members: ${clientSet.size}`);
        if (clientSet.size === 0) {
            rooms.delete(ws.currentRoom);
        }
        ws.currentRoom = null;
    }
}

// Защита от зависших TCP-соединений каждые 30 секунд
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(interval);
});

console.log(`[CommonTags Relay] Server started on port ${PORT}`);
