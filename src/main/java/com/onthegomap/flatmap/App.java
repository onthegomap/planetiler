package com.onthegomap.flatmap;

public record App() {

  record Greeting(int x) {

  }

  public static void main(String[] args) {
    System.out.println(new App().getGreeting());
  }

  public String getGreeting() {
    return "hello world " + new Greeting(1);
  }
}
