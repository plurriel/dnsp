import 'dotenv/config';

import { query } from 'dns-query';
import amqplib from 'amqplib';

import cuid2 from '@paralleldrive/cuid2';

import { INTEGER, STRING, Sequelize } from 'sequelize';

import { InferType, array, object, string } from 'yup';
import { Answer } from '@leichtgewicht/dns-packet';

if (!('AMQP_URL' in process.env) || typeof process.env.AMQP_URL !== 'string') throw new Error('AMQP_URL is a required string in env');
const conn = await amqplib.connect(process.env.AMQP_URL);

if (!('PERSIST_FILE' in process.env) || typeof process.env.PERSIST_FILE !== 'string') throw new Error('PERSIST_FILE is a required string in env');
const persistDb = new Sequelize(process.env.PERSIST_FILE);

const QueueItemModel = persistDb.define('queue_item', {
  name: {
    type: STRING(),
    allowNull: false,
  },
  type: {
    type: STRING(),
    allowNull: false,
  },
  value: {
    type: STRING(),
    allowNull: false,
  },
  transactionId: {
    type: STRING(),
    allowNull: false,
  },
  backoffExp: {
    type: INTEGER(),
    allowNull: false,
  },
}, { timestamps: false });
QueueItemModel.removeAttribute('id');
await QueueItemModel.sync();

const TransactionModel = persistDb.define('transaction', {
  id: {
    type: STRING(),
    allowNull: false,
    primaryKey: true,
  },
  callback: {
    type: STRING(),
    allowNull: false,
  },
  remaining: {
    type: INTEGER(),
    allowNull: false,
  },
}, { timestamps: false });
await TransactionModel.sync();

const dnsPollingQueue = 'dns_polling';
const dnsPollingAnswerQueue = 'dns_polling_ans';

const channel = await conn.createChannel();
channel.assertQueue(dnsPollingQueue);

const transactionCompletionRequirements = new Map<string, [number, string]>(
  (await TransactionModel.findAll())
    .map((transactionModel) => [
      transactionModel.getDataValue('id'),
      [transactionModel.getDataValue('remaining'), transactionModel.getDataValue('callback')],
    ])
);

const singleCheckNoTrId = object({
  name: string().required(),
  type: string().required(),
  value: string().required(),
}).required();

const singleCheck = object({
  name: string().required(),
  type: string().required(),
  value: string().required(),
  transactionId: string().required(),
}).required();

const messageSchema = object({
  transactionCheck: array(singleCheckNoTrId).required(),
  callback: string().required(),
}).required();

// Queue and burst
const backoffBase = 2;

type QueueProcessableItem = InferType<typeof singleCheck> & {
  value: string;
  backoffExp: number; // Store by how much we need to space the next request
};
type QueueItem = QueueProcessableItem[]
const DNSQueue: QueueItem[] = [
  (await QueueItemModel.findAll())
    .map((queueItemModel) => queueItemModel.dataValues),
];

console.log(DNSQueue);

export function processAnswer(answer: Answer): string {
  switch (answer.type) {
    case 'A':
    case 'AAAA':
    case 'CNAME':
    case 'DNAME':
    case 'NS':
    case 'PTR':
      return answer.data;
    case 'TXT':
      if (!Array.isArray(answer.data)) return answer.data.toString();
      return answer.data.map((chunk) => chunk.toString()).join('');
    case 'MX':
      return answer.data.exchange;
    default:
      throw new Error(`Dear dev, please implement ${answer.type}. This is what it looks like: ${JSON.stringify(answer)}`);
  }
}

const dequeueDelay = 1000;

const handleQueue = (quietRun = false) => {
  const first = DNSQueue.shift();
  if (first && first.length) {
    let processIndex = 0;
    let intervalId: NodeJS.Timeout;

    const intervalRunner = () => {
      if (processIndex >= first.length) return clearInterval(intervalId);
      const dnsQuery = first[processIndex];

      query({
        question: {
          name: dnsQuery.name,
          type: dnsQuery.type,
        },
      }, { endpoints: ['1.1.1.1'] })
        .then(async (result) => {
          const relevantAnswer = result.answers?.find((answer) => processAnswer(answer) === dnsQuery.value);
          if (!quietRun && relevantAnswer) {
            const previousTxRemaining = transactionCompletionRequirements.get(dnsQuery.transactionId)!;
            transactionCompletionRequirements.set(dnsQuery.transactionId, [
              previousTxRemaining[0] - 1,
              previousTxRemaining[1],
            ]);
            TransactionModel.update({
              remaining: previousTxRemaining[0] - 1,
            }, {
              where: { id: dnsQuery.transactionId },
            });

            QueueItemModel.destroy({
              where: dnsQuery,
              limit: 1,
            });

            if (previousTxRemaining[0] === 1) {
              fetch(previousTxRemaining[1]);
              transactionCompletionRequirements.delete(dnsQuery.transactionId);
              TransactionModel.destroy({
                where: {
                  id: dnsQuery.transactionId,
                },
                limit: 1,
              });
            }
          } else {
            const nextIdx = backoffBase ** dnsQuery.backoffExp;
            if (!DNSQueue[nextIdx]) DNSQueue[nextIdx] = [];
            DNSQueue[nextIdx].push({ ...dnsQuery, backoffExp: dnsQuery.backoffExp + 1 });
            QueueItemModel.update({ backoffExp: dnsQuery.backoffExp + 1 }, {
              where: dnsQuery,
              limit: 1,
            });
          }
        });
      processIndex += 1;
    };
    intervalId = setInterval(intervalRunner, dequeueDelay / first.length);
    intervalRunner();
  }
  setTimeout(handleQueue, dequeueDelay);
};

handleQueue(true);

await channel.consume(dnsPollingQueue, async (msg) => {
  if (!msg) return;
  const messageObj = JSON.parse(msg.content.toString());
  const { transactionCheck: message, callback } = messageSchema.validateSync(messageObj);

  if (!DNSQueue[0]) DNSQueue[0] = [];
  const transactionId = cuid2.createId();
  const dnsQueries = message.map((query) => ({
    name: query.name,
    type: query.type,
    value: query.value,
    transactionId,
    backoffExp: 0,
  }));
  transactionCompletionRequirements.set(transactionId, [
    dnsQueries.length,
    callback,
  ]);
  DNSQueue[0].push(...dnsQueries);
  console.log(dnsQueries);
  (async () => {
    const t = await persistDb.transaction();
    await Promise.all([
      TransactionModel.create({
        id: transactionId,
        remaining: dnsQueries.length,
        callback,
      }, { transaction: t }),
      QueueItemModel.bulkCreate(dnsQueries, { transaction: t }),
    ]);
    await t.commit();
    channel.ack(msg);
  })();
});

console.log(`Now consuming: ${dnsPollingQueue}. Outputting results in ${dnsPollingAnswerQueue}`)

// await query({
//   question: {
//     type: 
//   },
// });
