import { useEffect, useRef, useState } from 'react';
import { io, Socket } from 'socket.io-client';
import { WorldCupMatchState, NBAGameState } from '@sport-tracking/schemas';

export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected';

interface SportData {
  worldcup: WorldCupMatchState | null;
  nba: NBAGameState | null;
  status: ConnectionStatus;
}

export function useSocket(): SportData {
  const [worldcup, setWorldcup] = useState<WorldCupMatchState | null>(null);
  const [nba, setNba]           = useState<NBAGameState | null>(null);
  const [status, setStatus]     = useState<ConnectionStatus>('connecting');
  const socketRef               = useRef<Socket | null>(null);

  useEffect(() => {
    // Connects to the same origin — nginx proxies /socket.io/ to the consumer
    const socket = io({ transports: ['websocket', 'polling'] });
    socketRef.current = socket;

    socket.on('connect',    () => setStatus('connected'));
    socket.on('disconnect', () => setStatus('disconnected'));
    socket.on('worldcup:state', (state: WorldCupMatchState) => setWorldcup(state));
    socket.on('nba:state',      (state: NBAGameState)        => setNba(state));

    return () => { socket.disconnect(); };
  }, []);

  return { worldcup, nba, status };
}
