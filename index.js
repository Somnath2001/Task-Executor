const express = require('express');
const rateLimit = require('express-rate-limit');
const Redis = require('ioredis');
const fs = require('fs');
const cluster = require('cluster');
const os = require('os');

const redisClient = new Redis();
const logFile = 'task_logs.txt';

const task = async (user_id) => {
  const log = `${user_id}-task completed at-${Date.now()}\n`;
  fs.appendFileSync(logFile, log);
};

const processTaskQueue = async (user_id) => {
  const queueKey = `taskQueue:${user_id}`;
  while (true) {
    const taskData = await redisClient.lpop(queueKey);
    if (!taskData) break;
    await task(taskData.user_id);
  }
};

if (cluster.isMaster) {
  const numCPUs = os.cpus().length;
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
} else {
  const app = express();
  app.use(express.json());

  const userRateLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 20, 
    keyGenerator: (req) => req.body.user_id,
    handler: async (req, res) => {
      const user_id = req.body.user_id;
      await redisClient.rpush(`taskQueue:${user_id}`, req.body);
      res.status(429).send('Rate limit exceeded, task queued.');
      setTimeout(() => processTaskQueue(user_id), 1000);
    }
  });

  app.post('/task', userRateLimiter, async (req, res) => {
    const user_id = req.body.user_id;
    const currentTasks = await redisClient.llen(`taskQueue:${user_id}`);
    if (currentTasks < 1) {
      await task(user_id);
    } else {
      await redisClient.rpush(`taskQueue:${user_id}`, req.body);
      setTimeout(() => processTaskQueue(user_id), 1000);
    }
    res.send('Task processed or queued.');
  });

  app.listen(3000, () => {
    console.log(`Worker ${process.pid} started`);
  });
}
