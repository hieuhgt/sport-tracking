import {
  WebSocketGateway,
  WebSocketServer,
  OnGatewayConnection,
  OnGatewayDisconnect,
} from '@nestjs/websockets';
import { Logger } from '@nestjs/common';
import { Server, Socket } from 'socket.io';
import { StateService } from '../state/state.service';

const FIXED_WC_MATCH = 'wc-match-1';
const FIXED_NBA_GAME = 'nba-game-1';

@WebSocketGateway({ cors: { origin: '*' }, transports: ['websocket', 'polling'] })
export class EventsGateway implements OnGatewayConnection, OnGatewayDisconnect {
  private readonly logger = new Logger(EventsGateway.name);

  @WebSocketServer()
  server!: Server;

  constructor(private stateService: StateService) {}

  async handleConnection(client: Socket): Promise<void> {
    this.logger.log(`Client connected: ${client.id}`);
    const [wcState, nbaState] = await Promise.all([
      this.stateService.getWorldCupState(FIXED_WC_MATCH),
      this.stateService.getNBAState(FIXED_NBA_GAME),
    ]);
    if (wcState)  client.emit('worldcup:state', wcState);
    if (nbaState) client.emit('nba:state', nbaState);
  }

  handleDisconnect(client: Socket): void {
    this.logger.log(`Client disconnected: ${client.id}`);
  }

  broadcastUpdate(sport: 'worldcup' | 'nba', state: unknown): void {
    this.server.emit(sport === 'worldcup' ? 'worldcup:state' : 'nba:state', state);
  }
}
