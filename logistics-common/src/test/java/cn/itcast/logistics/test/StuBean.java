package cn.itcast.logistics.test;

import java.util.Objects;

public class StuBean {




    private Integer id;
    private String name;

    public StuBean() {
    }

    public StuBean(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StuBean stuBean = (StuBean) o;
        return Objects.equals(id, stuBean.id) &&
                Objects.equals(name, stuBean.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return "StuBean{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
