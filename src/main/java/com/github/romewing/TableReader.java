package com.github.romewing;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

public class TableReader {
    private JdbcTemplate jdbcTemplate;
    private String sql;

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Map<String, Object>> read() {
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(this.sql);
        return maps;
    }
}
