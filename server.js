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
function