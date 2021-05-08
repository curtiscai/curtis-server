package com.curtis.elasticsearch.raw.entity;

import java.math.BigDecimal;

/**
 * @author curtis.cai
 * @desc 用户实体类
 * @date 2021-05-07
 * @email curtis.cai@outlook.com
 * @reference
 */
public class User {

    private String name;

    private Boolean sex;

    private String birth;

    private Long phone;

    private BigDecimal height;

    private String desc;

    public User() {
    }

    public User(String name, Boolean sex, String birth, Long phone, BigDecimal height, String desc) {
        this.name = name;
        this.sex = sex;
        this.birth = birth;
        this.phone = phone;
        this.height = height;
        this.desc = desc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getSex() {
        return sex;
    }

    public void setSex(Boolean sex) {
        this.sex = sex;
    }

    public String getBirth() {
        return birth;
    }

    public void setBirth(String birth) {
        this.birth = birth;
    }

    public Long getPhone() {
        return phone;
    }

    public void setPhone(Long phone) {
        this.phone = phone;
    }

    public BigDecimal getHeight() {
        return height;
    }

    public void setHeight(BigDecimal height) {
        this.height = height;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", sex=" + sex +
                ", birth=" + birth +
                ", phone=" + phone +
                ", height=" + height +
                ", desc='" + desc + '\'' +
                '}';
    }
}
