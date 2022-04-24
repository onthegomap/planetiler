package com.onthegomap.planetiler.reader.osm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class OsmPhaserTest {

  @Test
  @Timeout(1)
  void testAdvanceSingleThread() {
    var phaser = new OsmPhaser(0);
    var forWorker = phaser.forWorker();
    assertEquals(OsmPhaser.Phase.BEGIN, phaser.getPhase());

    // multiple calls stays put
    forWorker.arrive(OsmPhaser.Phase.NODES);
    assertEquals(OsmPhaser.Phase.NODES, phaser.getPhase());
    assertEquals(0, phaser.nodes());
    forWorker.arrive(OsmPhaser.Phase.NODES);
    assertEquals(OsmPhaser.Phase.NODES, phaser.getPhase());
    assertEquals(1, phaser.nodes());

    forWorker.arriveAndWaitForOthers(OsmPhaser.Phase.WAYS);
    assertEquals(2, phaser.nodes());
    assertEquals(OsmPhaser.Phase.WAYS, phaser.getPhase());
    forWorker.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
    assertEquals(OsmPhaser.Phase.RELATIONS, phaser.getPhase());
    forWorker.close();
    assertEquals(OsmPhaser.Phase.DONE, phaser.getPhase());

    assertEquals(2, phaser.nodes());
    assertEquals(1, phaser.ways());
    assertEquals(1, phaser.relations());
  }

  @Test
  @Timeout(1)
  void testAdvanceAndSkipPhases() {
    var phaser = new OsmPhaser(1);
    var forWorker = phaser.forWorker();
    assertEquals(OsmPhaser.Phase.BEGIN, phaser.getPhase());

    forWorker.arrive(OsmPhaser.Phase.NODES);
    assertEquals(0, phaser.nodes(), "don't increment count before processing an element");
    assertEquals(OsmPhaser.Phase.NODES, phaser.getPhase());

    forWorker.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
    assertEquals(1, phaser.nodes(), "increment count until after processing an element");
    assertEquals(OsmPhaser.Phase.RELATIONS, phaser.getPhase());

    forWorker.close();

    assertEquals(1, phaser.nodes());
    assertEquals(0, phaser.ways());
    assertEquals(1, phaser.relations());
    assertEquals(OsmPhaser.Phase.DONE, phaser.getPhase());
  }

  @Test
  @Timeout(1)
  void testWorkerAdvanceSideEffect() {
    var nodesCalled = new AtomicBoolean(false);
    var waysCalled = new AtomicBoolean(false);
    var phaser = new OsmPhaser(0);

    var forWorker = phaser.forWorker()
      .whenWorkerFinishes(OsmPhaser.Phase.NODES, () -> nodesCalled.set(true))
      .whenWorkerFinishes(OsmPhaser.Phase.WAYS, () -> waysCalled.set(true));
    assertFalse(nodesCalled.get());
    assertFalse(waysCalled.get());

    forWorker.arrive(OsmPhaser.Phase.WAYS);
    assertTrue(nodesCalled.get());
    assertFalse(waysCalled.get());

    forWorker.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
    assertTrue(waysCalled.get());

    forWorker.close();
  }

  @Test
  @Timeout(1)
  void testCantGoBackwards() {
    var phaser = new OsmPhaser(1);
    var forWorker = phaser.forWorker();
    forWorker.arrive(OsmPhaser.Phase.WAYS);
    assertThrows(IllegalStateException.class, () -> forWorker.arrive(OsmPhaser.Phase.NODES));
    forWorker.close();
    assertThrows(IllegalStateException.class, () -> forWorker.arrive(OsmPhaser.Phase.RELATIONS));
  }

  @Test
  @Timeout(1)
  void testWaitToAdvance() throws InterruptedException {
    var phaser = new OsmPhaser(2);
    var latch1 = new CountDownLatch(1);
    var latch2 = new CountDownLatch(1);
    var latch3 = new CountDownLatch(1);
    var workingOnRelations = new AtomicBoolean(false);
    var workingOnWays = new AtomicBoolean(false);


    Thread t1 = new Thread(() -> {
      try (var worker = phaser.forWorker()) {
        worker.arrive(OsmPhaser.Phase.NODES);
        worker.arrive(OsmPhaser.Phase.WAYS);
        latch2.countDown();
        workingOnWays.set(true);
        worker.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
        workingOnRelations.set(true);
        worker.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
        worker.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    Thread t2 = new Thread(() -> {
      try (var worker = phaser.forWorker()) {
        worker.arrive(OsmPhaser.Phase.NODES);
        latch1.await();
        worker.arrive(OsmPhaser.Phase.WAYS);
        latch3.await();
        worker.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
        worker.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
        worker.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    });
    t1.start();
    t2.start();

    latch2.await(); // proves that thread 1 advanced to ways before thread 2 did
    latch1.countDown(); // let thread 2 continue on to ways

    // wait and make sure thread 1 is still waiting on arriveAndWaitForOthers(RELATIONS)
    Thread.sleep(100);
    assertTrue(workingOnWays.get());
    assertFalse(workingOnRelations.get());

    latch3.countDown(); // now let thread 2 move onto relations, unblock thread 1 and they both finish

    t1.join();
    t2.join();
    assertEquals(OsmPhaser.Phase.DONE, phaser.getPhase());
    assertEquals(2, phaser.nodes());
    assertEquals(2, phaser.ways());
    assertEquals(6, phaser.relations());
  }
}
