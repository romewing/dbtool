package com.github.romewing;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.AbstractSqlPagingQueryProvider;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EnableBatchProcessing
@SpringBootApplication
public class DBToolApplication {

    private static final String OVERRIDDEN_BY_EXPRESSION = null;
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Autowired(required = false)
    private MongoOperations mongoOperations;

    @Value("${table}")
    private String table;

    @Autowired
    private RowPartitioner partitioner;


    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(100);
        taskExecutor.setCorePoolSize(7);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    @Bean
    @StepScope
    public ItemReader<Object> reader(@Value("#{stepExecutionContext[partition]}") String partition) {
        JdbcPagingItemReader<Object> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setQueryProvider(queryProvider());
        reader.setPageSize(100000);
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("partition", partition);
        reader.setParameterValues(parameterValues);
        reader.setRowMapper(columnMapRowMapper());
        try {
            reader.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return reader;
    }


    @Bean
    public JdbcCursorItemReader jdbcCursorItemReader() {
        JdbcCursorItemReader reader = new JdbcCursorItemReader();
        reader.setSql("select * from evs_rs_collect_value_day where record_time between(?,?)");
        reader.setRowMapper(columnMapRowMapper());
        reader.setMaxRows(10000);
        reader.setDataSource(dataSource);
        return reader;
    }

    public RowMapper columnMapRowMapper() {
        RowMapper columnMapRowMapper = new ColumnMapRowMapper();
        return columnMapRowMapper;
    }

    @Bean
    public PagingQueryProvider queryProvider() {
        AbstractSqlPagingQueryProvider queryProvider =  new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("*");
        queryProvider.setFromClause(table);
        queryProvider.setWhereClause("WEEKDAY(record_time) = :partition");
        Map<String, Order> sortKeys = new HashMap<>();
        //sortKeys.put("record_time", Order.ASCENDING);
        //sortKeys.put("unixtime", Order.ASCENDING);
        sortKeys.put("record_id", Order.ASCENDING);
        queryProvider.setSortKeys(sortKeys);
        return queryProvider;
    }


    @Bean
    @StepScope
    public ItemWriter<Object> writer() {
       return new ItemWriter<Object>() {
            @Override
            public void write(List items) throws Exception {
                System.out.println(items);
            }
        };
    }

   @Bean
   @StepScope
    public MongoItemWriter mongoItemWriter() {
        MongoItemWriter writer = new MongoItemWriter();
        writer.setTemplate(mongoOperations);
        writer.setCollection("collectValueDay");
        return writer;
    }

    @Bean
    public Step step(){
        return stepBuilderFactory.get("step").allowStartIfComplete(true).partitioner(partitionStep()).partitioner("partition-step", partitioner).gridSize(7).taskExecutor(taskExecutor()).build();
    }

    public Step partitionStep() {
        return stepBuilderFactory.get("partition-step").allowStartIfComplete(true).chunk(100000).reader(reader(OVERRIDDEN_BY_EXPRESSION)).writer(mongoItemWriter()).build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job").incrementer(new RunIdIncrementer()).flow(step()).end().build();
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext run =
                SpringApplication.run(DBToolApplication.class);
        Job job = run.getBean(Job.class);
        try {
            run.getBean(JobLauncher.class).run(job, new JobParameters()).getExitStatus();
            int exit = SpringApplication.exit(run);
            System.exit(exit);
        } catch (JobExecutionAlreadyRunningException e) {
            e.printStackTrace();
        } catch (JobRestartException e) {
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            e.printStackTrace();
        }
    }
}
