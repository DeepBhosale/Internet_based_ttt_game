// server.js
// Multi-room Tic-Tac-Toe server with chat, scoreboard, net-sim and room discovery (lobby)
const express = require('express');
const http = require('http');
const path = require('path');
const app = express();
const server = http.createServer(app);
const { Server } = require('socket.io');
const io = new Server(server);

// Serve static client
app.use(express.static(path.join(__dirname, 'public')));

// Port
const PORT = process.env.PORT || 3000;

// In-memory rooms map: roomCode -> roomState
const rooms = new Map();

/**
 * Room state structure:
 * {
 *  roomCode,
 *  players: [{ socketId, playerId, name, symbol, color }],
 *  board: Array(9),
 *  turn: 'X'|'O'|null,
 *  scores: { playerId: wins },
 *  chat: [ { playerId, name, color, text, ts } ],
 *  createdAt: timestamp
 * }
 */

// --- NETWORK SIMULATION (optional) ---
const simSettings = new Map();
function getSim(roomCode) {
  return simSettings.get(roomCode) || { enabled: false, latency: 0, jitter: 0, loss: 0 };
}
function maybeDrop(lossPercent) {
  if (!lossPercent) return false;
  return Math.random() < (lossPercent / 100);
}
function scheduleEmit(roomCode, event, data) {
  const sim = getSim(roomCode);
  if (sim.enabled && maybeDrop(sim.loss)) {
    console.log(`[NET-SIM] DROP event="${event}" room=${roomCode}`);
    return;
  }
  const baseDelay = sim.enabled ? Number(sim.latency || 0) : 0;
  const jitter = sim.enabled ? ((Math.random() * 2 - 1) * Number(sim.jitter || 0)) : 0;
  const delay = Math.max(0, Math.round(baseDelay + jitter));
  if (delay > 0) {
    const payloadCopy = (data && typeof data === 'object') ? JSON.parse(JSON.stringify(data)) : data;
    setTimeout(() => {
      io.to(roomCode).emit(event, payloadCopy);
    }, delay);
    console.log(`[NET-SIM] DELAY ${delay}ms event="${event}" room=${roomCode}`);
  } else {
    io.to(roomCode).emit(event, data);
  }
}
// --- end net-sim ---

// Helpers
function makeRoomState(code) {
  return {
    roomCode: code,
    players: [],
    board: Array(9).fill(''),
    turn: 'X',
    lastUpdate: Date.now(),
    scores: {},
    createdAt: Date.now(),
    chat: []
  };
}

function generateRoomCode(len = 4) {
  const chars = 'ABCDEFGHJKMNPQRSTUVWXY3456789';
  let s = '';
  for (let i = 0; i < len; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

function assignSymbols(room) {
  room.players.forEach((p, i) => {
    if (i === 0) p.symbol = 'X';
    else if (i === 1) p.symbol = 'O';
    else p.symbol = 'spectator';
  });
}

function checkWinner(board) {
  const lines = [
    [0,1,2],[3,4,5],[6,7,8],
    [0,3,6],[1,4,7],[2,5,8],
    [0,4,8],[2,4,6]
  ];
  for (const [a,b,c] of lines) {
    if (board[a] && board[a] === board[b] && board[a] === board[c]) return board[a];
  }
  if (board.every(x => x)) return 'draw';
  return null;
}

function resetBoard(room, preservePlayers = true) {
  const oldPlayers = preservePlayers ? room.players.slice(0,2).map((p,i)=>({ socketId: p.socketId, playerId: p.playerId, name: p.name, color: p.color, symbol: i===0 ? 'X' : 'O' })) : [];
  room.board = Array(9).fill('');
  room.turn = 'X';
  room.lastUpdate = Date.now();
  if (preservePlayers) {
    const others = room.players.slice(2).map(p => ({ socketId: p.socketId, playerId: p.playerId, name: p.name, color: p.color, symbol: 'spectator' }));
    room.players = oldPlayers.concat(others);
  } else {
    room.players = [];
  }
}

// Chat helper: push message, cap list
function pushChat(room, msg) {
  if (!room.chat) room.chat = [];
  room.chat.push(msg);
  if (room.chat.length > 100) room.chat.splice(0, room.chat.length - 100);
}

// find room by socketId
function findRoomBySocket(socketId) {
  for (const [code, room] of rooms.entries()) {
    if (room.players.some(p => p.socketId === socketId)) return room;
  }
  return null;
}

function sanitizeName(name) {
  if (!name) return 'Player';
  return String(name).slice(0,20).replace(/[<>]/g, '');
}

function randomColor() {
  const pal = ['#EF4444','#F97316','#F59E0B','#EAB308','#10B981','#06B6D4','#3B82F6','#8B5CF6','#EC4899','#F43F5E'];
  return pal[Math.floor(Math.random() * pal.length)];
}

// --------- Room discovery / lobby utilities ----------
function buildRoomsList() {
  // Return array of lightweight room descriptors for the lobby
  const arr = [];
  for (const [code, room] of rooms.entries()) {
    arr.push({
      roomCode: code,
      players: room.players.length,
      createdAt: room.createdAt
    });
  }
  // sort by active players desc then recent
  arr.sort((a,b) => {
    if (b.players !== a.players) return b.players - a.players;
    return b.createdAt - a.createdAt;
  });
  return arr;
}

function broadcastRoomsList() {
  const list = buildRoomsList();
  io.emit('rooms_list', list); // broadcast to all connected sockets
  console.log('[LOBBY] broadcast rooms_list count=', list.length);
}

// Remove a room if it has no players (cleanup)
function cleanupRoomIfEmpty(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  if (!room.players || room.players.length === 0) {
    rooms.delete(roomCode);
    console.log(`[ROOM] deleted empty room ${roomCode}`);
    broadcastRoomsList();
  }
}
// ----------------------------------------------------

// Socket logic
io.on('connection', (socket) => {
  console.log('connect', socket.id);

  // When a client asks for current rooms (callback)
  socket.on('list_rooms', (payload, cb) => {
    const list = buildRoomsList();
    cb && cb({ ok:true, rooms: list });
  });

  // Create a new room, returns code
  socket.on('create_room', ({ requestedCode } = {}, cb) => {
    let code = requestedCode && String(requestedCode).toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,6);
    if (!code || rooms.has(code)) {
      let tries = 0;
      do {
        code = generateRoomCode(4);
        tries++;
      } while (rooms.has(code) && tries < 10);
    }
    const room = makeRoomState(code);
    rooms.set(code, room);
    console.log(`[ROOM] created ${code}`);
    broadcastRoomsList(); // notify lobby
    cb && cb({ ok: true, code });
  });

  // Join a room
  socket.on('join_room', (payload, cb) => {
    try {
      const roomCode = String(payload.roomCode || '').toUpperCase();
      const name = sanitizeName(payload.name) || ('Player-' + (payload.playerId ? payload.playerId.slice(-4) : socket.id.slice(-4)));
      const playerId = payload.playerId || ('pid-' + Math.random().toString(36).slice(2,9));
      const color = payload.color || null;

      if (!roomCode) return cb && cb({ ok:false, error:'missing room code' });

      let room = rooms.get(roomCode);
      if (!room) {
        // auto-create room if not exists
        room = makeRoomState(roomCode);
        rooms.set(roomCode, room);
      }

      // remove old playerId (reconnect)
      room.players = room.players.filter(p => p.playerId !== playerId);

      const player = {
        socketId: socket.id,
        playerId,
        name,
        color: color || randomColor(),
        symbol: 'spectator'
      };
      room.players.push(player);
      assignSymbols(room);
      if (!room.scores[playerId]) room.scores[playerId] = 0;

      socket.join(roomCode);
      room.lastUpdate = Date.now();

      // send updated room state to everyone in room
      scheduleEmit(roomCode, 'state', room);

      // update lobby list for everyone
      broadcastRoomsList();

      cb && cb({ ok:true, state: room, playerId, color: player.color });
    } catch (err) {
      console.error('join_room error', err);
      cb && cb({ ok:false, error: 'server error' });
    }
  });

  // Move: { roomCode, idx, playerId }
  socket.on('move', (payload) => {
    const roomCode = String(payload.roomCode || '').toUpperCase();
    const idx = payload.idx;
    const playerId = payload.playerId;
    if (!rooms.has(roomCode)) return;
    const room = rooms.get(roomCode);
    const player = room.players.find(p => p.playerId === playerId);
    if (!player) return;
    if (player.symbol !== 'X' && player.symbol !== 'O') return;
    if (room.turn !== player.symbol) return;
    if (typeof idx !== 'number' || idx<0 || idx>8) return;
    if (room.board[idx]) return;

    room.board[idx] = player.symbol;
    const winner = checkWinner(room.board);
    if (winner && winner!=='draw') {
      const winnerPlayer = room.players.find(p=>p.symbol === winner);
      if (winnerPlayer) {
        room.scores[winnerPlayer.playerId] = (room.scores[winnerPlayer.playerId] || 0) + 1;
      }
      room.turn = null;
    } else if (winner === 'draw') {
      room.turn = null;
    } else {
      room.turn = (room.turn === 'X') ? 'O' : 'X';
    }
    room.lastUpdate = Date.now();

    scheduleEmit(roomCode, 'state', room);
  });

  // Reset (client asks to reset; server will reset and broadcast)
  socket.on('reset_room', (payload) => {
    const roomCode = String(payload.roomCode || '').toUpperCase();
    if (!rooms.has(roomCode)) return;
    const room = rooms.get(roomCode);
    resetBoard(room, true);
    room.lastUpdate = Date.now();
    scheduleEmit(roomCode, 'state', room);
  });

  // Request current state for a room
  socket.on('request_state', (payload, cb) => {
    const roomCode = String(payload.roomCode || '').toUpperCase();
    if (!rooms.has(roomCode)) return cb && cb({ ok:false, error:'no such room' });
    const room = rooms.get(roomCode);
    cb && cb({ ok:true, state:room });
  });

  // Leave (explicit leave)
  socket.on('leave_room', (payload) => {
    const roomCode = String(payload.roomCode || '').toUpperCase();
    const playerId = payload.playerId;
    if (!rooms.has(roomCode)) return;
    const room = rooms.get(roomCode);
    room.players = room.players.filter(p => p.playerId !== playerId);
    assignSymbols(room);
    room.lastUpdate = Date.now();
    scheduleEmit(roomCode, 'state', room);
    cleanupRoomIfEmpty(roomCode);
    broadcastRoomsList();
  });

  // Chat message
  socket.on('chat_message', (payload, cb) => {
    try {
      const roomCode = String(payload.roomCode || '').toUpperCase();
      if (!rooms.has(roomCode)) return cb && cb({ ok:false, error:'no such room' });
      const room = rooms.get(roomCode);
      const player = room.players.find(p=>p.playerId === payload.playerId);
      const name = player ? player.name : (payload.name || 'Guest');
      const color = player ? player.color : (payload.color || randomColor());
      const text = String(payload.text || '').trim().slice(0,1000);
      if (!text) return cb && cb({ ok:false, error:'empty message' });

      const msg = { playerId: payload.playerId, name, color, text, ts: Date.now() };
      pushChat(room, msg);

      scheduleEmit(roomCode, 'chat', msg);

      cb && cb({ ok:true, message: msg });
    } catch (err) {
      console.error('chat_message error', err);
      cb && cb({ ok:false, error:'server error' });
    }
  });

  // set network sim (optional)
  socket.on('set_net_sim', (payload, cb) => {
    try {
      const roomCode = String(payload.roomCode || '').toUpperCase();
      if (!rooms.has(roomCode)) return cb && cb({ ok: false, error: 'no such room' });

      const newSettings = {
        enabled: !!payload.enabled,
        latency: Math.max(0, Number(payload.latency) || 0),
        jitter: Math.max(0, Number(payload.jitter) || 0),
        loss: Math.min(100, Math.max(0, Number(payload.loss) || 0))
      };

      simSettings.set(roomCode, newSettings);
      console.log(`[NET-SIM] room=${roomCode} set`, newSettings);

      scheduleEmit(roomCode, 'net_sim_update', newSettings);

      cb && cb({ ok: true, settings: newSettings });
    } catch (err) {
      console.error('set_net_sim error', err);
      cb && cb({ ok: false, error: 'server error' });
    }
  });

  // On disconnect: remove player from room if present
  socket.on('disconnect', () => {
    const room = findRoomBySocket(socket.id);
    if (room) {
      room.players = room.players.filter(p => p.socketId !== socket.id);
      assignSymbols(room);
      room.lastUpdate = Date.now();
      scheduleEmit(room.roomCode, 'state', room);
      cleanupRoomIfEmpty(room.roomCode);
      broadcastRoomsList();
    }
    console.log('disconnect', socket.id);
  });
});

// Start server
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}. Open on LAN at: http://<this-machine-ip>:${PORT}`);
});
