package com.curtis.elasticsearch.model;

import java.math.BigDecimal;
import java.util.Date;

public class User {

    private String name;

    private Date birth;

    private Boolean sex;

    private BigDecimal height;

    public User() {
    }

    public User(String name, Date birth, Boolean sex, BigDecimal height) {
        this.name = name;
        this.birth = birth;
        this.sex = sex;
        this.height = height;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getBirth() {
        return birth;
    }

    public void setBirth(Date birth) {
        this.birth = birth;
    }

    public Boolean getSex() {
        return sex;
    }

    public void setSex(Boolean sex) {
        this.sex = sex;
    }

    public BigDecimal getHeight() {
        return height;
    }

    public void setHeight(BigDecimal height) {
        this.height = height;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", birth=" + birth +
                ", sex=" + sex +
                ", height=" + height +
                '}';
    }
}
