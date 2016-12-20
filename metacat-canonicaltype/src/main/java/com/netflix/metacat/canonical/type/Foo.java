package com.netflix.metacat.canonical.type;

import lombok.Data;

/**
 * Created by zhenli on 12/20/16.
 */
@Data
public class Foo {
    private String name;
    private String address;

    public static void main(String[] args){
        Foo f = new Foo();
        f.setAddress("abc");
    }
}
