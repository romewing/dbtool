package com.github.romewing;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.AbstractSqlPagingQueryProvider;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EnableBatchProcessing
@SpringBootApplication
public class DBToolApplication {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean
    public ItemReader<Object> reader() {
        JdbcPagingItemReader<Object> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setQueryProvider(queryProvider());
        reader.setRowMapper(rowMapper());
        return reader;
    }

    @Bean
    public RowMapper rowMapper() {
        return new ColumnMapRowMapper();
    }

    @Bean
    public PagingQueryProvider queryProvider() {
        AbstractSqlPagingQueryProvider queryProvider =  new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("*");
        queryProvider.setFromClause("bid_project");
        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("start_date", Order.ASCENDING);
        queryProvider.setSortKeys(sortKeys);
        return queryProvider;
    }


    public ItemWriter<Object> writer() {
        return new ItemWriter<Object>() {
            @Override
            public void write(List<?> items) throws Exception {
                System.out.println(items);
            }
        };

    }

    @Bean
    public Step step(){
        return stepBuilderFactory.get("step").chunk(10).reader(reader()).writer(writer()).build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job").incrementer(new RunIdIncrementer()).flow(step()).end().build();
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext run =
                SpringApplication.run(DBToolApplication.class);
        PlatformTransactionManager bean = run.getBean(PlatformTransactionManager.class);
        System.out.println(bean);
    }
}
