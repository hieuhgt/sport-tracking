import { Controller, Get, Post } from '@nestjs/common';
import { SimulatorService } from './simulator.service';

@Controller('simulate')
export class SimulatorController {
  constructor(private simulatorService: SimulatorService) {}

  @Post('start')
  start() {
    this.simulatorService.start();
    return { ok: true, message: 'Simulator started' };
  }

  @Post('stop')
  stop() {
    this.simulatorService.stop();
    return { ok: true, message: 'Simulator stopped' };
  }

  @Post('reset')
  async reset() {
    await this.simulatorService.reset();
    return { ok: true, message: 'Simulator reset' };
  }

  @Get('status')
  status() {
    return { running: this.simulatorService.isRunning() };
  }
}
