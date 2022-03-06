package com.onthegomap.planetiler.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class DistributorTest {

  @Test
  @Timeout(10)
  public void testEmpty() {
    List<Integer> processed = new CopyOnWriteArrayList<>();
    Distributor<Integer> distributor = Distributor.createWithCapacity(1);

    var thisDistributor = distributor.forThread(processed::add);
    thisDistributor.close();

    thisDistributor.close();

    assertThrows(IllegalStateException.class, () -> thisDistributor.accept(3));
    assertEquals(List.of(), processed);
  }

  @Test
  @Timeout(10)
  public void testDistributor1Thread() {
    List<Integer> processed = new CopyOnWriteArrayList<>();
    Distributor<Integer> distributor = Distributor.createWithCapacity(1);

    var thisDistributor = distributor.forThread(processed::add);

    assertEquals(List.of(), processed);
    thisDistributor.accept(1);
    assertEquals(List.of(1), processed);
    thisDistributor.accept(2);
    assertEquals(List.of(1, 2), processed);

    thisDistributor.close();

    assertThrows(IllegalStateException.class, () -> thisDistributor.accept(3));
    assertEquals(List.of(1, 2), processed);
  }

  @Test
  @Timeout(10)
  public void testDistributor2Threads() throws InterruptedException {
    List<Integer> processed = new CopyOnWriteArrayList<>();
    Distributor<Integer> distributor = Distributor.createWithCapacity(1);

    CountDownLatch a = new CountDownLatch(1);
    CountDownLatch b = new CountDownLatch(1);
    CountDownLatch c = new CountDownLatch(1);
    CountDownLatch d = new CountDownLatch(1);
    CountDownLatch e = new CountDownLatch(1);
    CountDownLatch f = new CountDownLatch(1);
    Thread thread1 =
        new Thread(
            () -> {
              try {
                var thisDistributor = distributor.forThread(processed::add);
                thisDistributor.accept(1);
                thisDistributor.accept(2);
                a.countDown();
                c.await();
                d.await();
                thisDistributor.accept(5);
                thisDistributor.accept(6); // queue full
                e.countDown();
                f.await();
                thisDistributor.close();
              } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
              }
            });
    thread1.start();
    Thread thread2 =
        new Thread(
            () -> {
              try {
                var thisDistributor = distributor.forThread(processed::add);
                a.await();
                thisDistributor.accept(3);
                thisDistributor.accept(4);
                b.countDown();
                thisDistributor.finish();
                d.countDown();
                f.await();
                thisDistributor.drain();
              } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
              }
            });
    thread2.start();

    b.await();
    assertEquals(List.of(1, 2, 3, 4), processed);
    c.countDown();
    e.await();
    assertEquals(List.of(1, 2, 3, 4, 6), processed);
    f.countDown();

    thread1.join();
    thread2.join();

    assertEquals(List.of(1, 2, 3, 4, 6, 5), processed);
  }
}
