package kr.co.mz.pmspsa;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

public class Main {

    public static void main(String[] args) {
        SchedulerFactory schedFact = new StdSchedulerFactory();

        try {
            Scheduler sched = schedFact.getScheduler();
            JobDetail jobA = JobBuilder.newJob(CorpASync.class).withIdentity("jobA", Scheduler.DEFAULT_GROUP).build();

            Trigger triggerA = null;

            if(args.length == 1) {
//             * Test ? 초 마다 실행
                triggerA = TriggerBuilder.newTrigger().withIdentity("triggerA", Scheduler.DEFAULT_GROUP).startNow().withPriority(15).withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(Integer.parseInt(args[0])*1000).repeatForever()).build();

            } else {
//             * 운영 매일 오전 5시
                triggerA = TriggerBuilder.newTrigger().withIdentity("triggerA", Scheduler.DEFAULT_GROUP).startNow().withPriority(15).withSchedule(CronScheduleBuilder.cronSchedule("0 0 5 * * ?")).build();
            }

            sched.scheduleJob(jobA, triggerA);
            sched.start();

        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }
}
