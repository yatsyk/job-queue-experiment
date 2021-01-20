import "reflect-metadata";
import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  getManager,
  OneToMany,
  ConnectionOptions,
  createConnection,
  BaseEntity,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
} from "typeorm";
import {
  ObjectType,
  ID,
  Resolver,
  Query,
  InputType,
  Field,
  buildSchema,
  Mutation,
  Arg,
} from "type-graphql";
import { ApolloError, ApolloServer } from "apollo-server";

enum JobStatus {
  waiting = 100,
  running = 200,
  finished = 300,
}

@InputType()
export class CreateJobInput {
  @Field()
  name: string;

  @Field({ nullable: true })
  inData?: string;

  @Field({ nullable: true })
  priority?: number;
}

@InputType()
export class CreateJobWorkerInput {
  @Field()
  name: string;
}

@InputType()
export class GetJobInput {
  @Field()
  workerId: number;
}

@InputType()
export class CreateLogMessageInput {
  @Field()
  jobId: number;

  @Field()
  workerId: number;

  @Field()
  text: string;
}

@Entity("workers")
@ObjectType()
export class JobWorker extends BaseEntity {
  @Field(() => ID)
  @PrimaryGeneratedColumn()
  id!: number;

  @Field(() => String)
  @Column({})
  name?: string;

  @Field(() => Date)
  @CreateDateColumn({})
  createdAt: Date;

  @Field(() => Date)
  @UpdateDateColumn({})
  updatedAt: Date;

  @Field(() => [LogMessage])
  @OneToMany(() => LogMessage, (log_message) => log_message.job_worker, {
    //eager: true,
    cascade: ["insert"],
  })
  log_messages: LogMessage[];

  @Field(() => [Job])
  @OneToMany(() => Job, (job) => job.job_worker, {
    //eager: true,
    cascade: ["insert"],
  })
  jobs: Job[];
}

@Entity("jobs")
@ObjectType()
export class Job extends BaseEntity {
  @Field(() => ID)
  @PrimaryGeneratedColumn()
  id!: number;

  @Field(() => String)
  @Column({})
  name?: string;

  @Field(() => Number)
  @Column({ default: 1000 })
  priority: number;

  @Field(() => Number)
  @Column({ default: JobStatus.waiting })
  status: JobStatus;

  @Field(() => Date)
  @CreateDateColumn({})
  createdAt: Date;

  @Field(() => Date)
  @UpdateDateColumn({})
  updatedAt: Date;

  @Field(() => String)
  @Column({ nullable: true })
  inData?: string;

  @Field(() => String)
  @Column({ nullable: true })
  outData?: string;

  @Field(() => [LogMessage])
  @OneToMany(() => LogMessage, (log_message) => log_message.job, {
    //eager: true,
    cascade: ["insert"],
  })
  log_messages: LogMessage[];

  @Field({ nullable: true })
  @ManyToOne(() => JobWorker, (job_worker) => job_worker.jobs, {
    //eager: true
  })
  job_worker?: JobWorker;
}

@Entity("log_messages")
@ObjectType()
export class LogMessage extends BaseEntity {
  @Field(() => ID)
  @PrimaryGeneratedColumn()
  id!: number;

  @Field(() => String)
  @Column({})
  text?: string;

  @Field(() => Date)
  @CreateDateColumn({})
  createdAt: Date;

  @Field(() => Date)
  @UpdateDateColumn({})
  updatedAt: Date;

  @Field()
  @ManyToOne(() => JobWorker, (job_worker) => job_worker.log_messages, {})
  job_worker: JobWorker;

  @Field()
  @ManyToOne(() => Job, (job) => job.log_messages, {})
  job: Job;
}

@Resolver()
class JobsResolver {
  @Query(() => [Job])
  jobs() {
    return Job.find({ relations: ["log_messages", "job_worker"] });
  }

  @Mutation(() => Job, { nullable: true })
  async getJob(@Arg("data") data: GetJobInput) {
    const job = await new Promise(async (resolve, reject) => {
      await getManager().transaction(async (manager) => {
        const job_worker = await manager.findOne(JobWorker, data.workerId);
        if (!job_worker) {
          reject(new ApolloError(`JobWorker ${data.workerId} not found`));
          return;
        }
        const job = await manager.findOne(Job, {
          where: { status: JobStatus.waiting },
          relations: ["log_messages", "job_worker"],
        });
        if (!job) {
          resolve(null);
          return;
        }
        job.job_worker = job_worker;
        job.status = JobStatus.running;
        await job.save();
        resolve(job);
      });
    });
    if (job) {
      return job;
    }
  }

  @Mutation(() => JobWorker)
  async createJobWorker(@Arg("data") data: CreateJobWorkerInput) {
    const jw = JobWorker.create({
      name: data.name,
    });
    await jw.save();
    return jw;
  }

  @Mutation(() => Job)
  async createJob(@Arg("data") data: CreateJobInput) {
    const job = Job.create({
      priority: data.priority || 1000,
      inData: data.inData,
      status: JobStatus.waiting,
      name: data.name,
    });
    await job.save();
    return job;
  }

  @Query(() => [LogMessage])
  logMessages() {
    return LogMessage.find({ relations: ["job", "job_worker"] });
  }

  @Mutation(() => LogMessage)
  async createLogMessage(@Arg("data") data: CreateLogMessageInput) {
    const job = await Job.findOne(data.jobId);
    const jobWorker = await JobWorker.findOne(data.workerId);
    if (!job) {
      throw new ApolloError(`Job ${data.jobId} not found`, "404");
    }
    if (!jobWorker) {
      throw new ApolloError(`Worker ${data.workerId} not found`, "404");
    }
    const message = LogMessage.create({
      text: data.text,
      job: job,
      job_worker: jobWorker,
    });
    //message.job = job;
    await message.save();
    return message;
  }
}

const options: ConnectionOptions = {
  type: "sqlite",
  database: `/tmp/data/line.sqlite`,
  entities: [Job, LogMessage, JobWorker],
  synchronize: true,
  logging: true,
};

async function main() {
  const connection = await createConnection(options);
  const PORT = process.env.PORT || 8000;
  const schema = await buildSchema({
    resolvers: [JobsResolver],
  });
  const server = new ApolloServer({ schema });
  await server.listen(PORT);
  console.log(`⚡️[server]: Server is running at https://localhost:${PORT}`);
}

main();