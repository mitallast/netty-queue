package org.mitallast.raft

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestActorRef, TestFSMRef, TestKit, TestProbe}
import org.mitallast.raft.Raft.WordConcatProtocol
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks

class RaftSpec extends TestKit(ActorSystem("RaftTest"))
  with ImplicitSender
  with DefaultTimeout
  with WordSpecLike
  with Matchers
  with TableDrivenPropertyChecks {

  import RaftProtocol._

  "raft log" should {

    import WordConcatProtocol._

    val term = Term(1)
    val log = ReplicatedLog.empty
    val entry1 = LogEntry(AppendWord("word"), term = term, index = 1)
    val entry2 = LogEntry(AppendWord("word"), term = term, index = 2)
    val entry3 = LogEntry(AppendWord("word"), term = term, index = 3)
    val clusterConf = StableClusterConfiguration(0, Set.empty)
    val snapshot1 = RaftSnapshot(RaftSnapshotMetadata(term, 1, clusterConf), data = Nil)
    val snapshot2 = RaftSnapshot(RaftSnapshotMetadata(term, 2, clusterConf), data = Nil)
    val snapshot3 = RaftSnapshot(RaftSnapshotMetadata(term, 3, clusterConf), data = Nil)
    val snapshotEntry1 = LogEntry(snapshot1, term, 1)
    val snapshotEntry2 = LogEntry(snapshot2, term, 2)
    val snapshotEntry3 = LogEntry(snapshot3, term, 3)

    "add first entry" in {
      (log + entry1).entries shouldEqual List(entry1)
    }
    "add second entry" in {
      (log + entry1 + entry2).entries shouldEqual List(entry1, entry2)
    }
    "match next entry" in {
      (log + entry1 + entry3).entries shouldEqual List(entry1, entry3)
    }
    "return 1 next index if empty" in {
      log.nextIndex shouldEqual 1
    }
    "return 2 next index contains entry 1" in {
      (log + entry1).nextIndex shouldEqual 2
    }
    "return 3 next index contains entry 2" in {
      (log + entry1 + entry2).nextIndex shouldEqual 3
    }
    "return 4 next index contains entry 3" in {
      (log + entry1 + entry2 + entry3).nextIndex shouldEqual 4
    }
    "return prev index 0 if empty" in {
      log.prevIndex shouldEqual 0
    }
    "return prev index 1 if contains entry 0" in {
      (log + entry1).prevIndex shouldEqual 0
    }
    "return prev index 1 if contains entry 2" in {
      (log + entry1 + entry2).prevIndex shouldEqual 1
    }
    "return prev index 2 if contains entry 3" in {
      (log + entry1 + entry2 + entry3).prevIndex shouldEqual 2
    }
    "return next entries lower bound 0" in {
      (log + entry1 + entry2 + entry3).entriesBatchFrom(0) shouldEqual List(entry1, entry2, entry3)
    }
    "return next entries lower bound 1" in {
      (log + entry1 + entry2 + entry3).entriesBatchFrom(1) shouldEqual List(entry2, entry3)
    }
    "return next entries lower bound 2" in {
      (log + entry1 + entry2 + entry3).entriesBatchFrom(2) shouldEqual List(entry3)
    }
    "return next entries lower bound 3" in {
      (log + entry1 + entry2 + entry3).entriesBatchFrom(3) shouldEqual List()
    }
    "return contains matching entry 0 if empty" in {
      log.containsMatchingEntry(Term(0), otherPrevIndex = 0) shouldBe true
    }
    "return contains matching entry 1 if empty" in {
      log.containsMatchingEntry(Term(0), otherPrevIndex = 1) shouldBe false
    }
    "return contains matching entry 1 if not empty" in {
      (log + entry1 + entry2).containsMatchingEntry(term, 2) shouldBe true
    }
    "return between 0, 0 empty" in {
      (log + entry1 + entry2 + entry3).between(0, 0) shouldEqual List()
    }
    "return between 0, 1 entry1" in {
      (log + entry1 + entry2 + entry3).between(0, 1) shouldEqual List(entry1)
    }
    "return between 1, 2 entry2" in {
      (log + entry1 + entry2 + entry3).between(1, 2) shouldEqual List(entry2)
    }
    "return between 1, 3 entry3" in {
      (log + entry1 + entry2 + entry3).between(1, 3) shouldEqual List(entry2, entry3)
    }
    "return commitIndex if empty" in {
      log.committedIndex shouldEqual 0
    }
    "return not contains entry 1 if empty" in {
      log.containsEntryAt(1) shouldBe false
    }
    "return contains entry 1 if entry 1" in {
      (log + entry1).containsEntryAt(1) shouldBe true
    }
    "compact log with" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot3)
      compacted.entries shouldEqual List(snapshotEntry3)
      compacted.hasSnapshot shouldEqual true
      compacted.snapshot shouldEqual snapshot3
    }
    "return contains matching entry after compaction 1" in {
      val compacted = (log + entry1).compactedWith(snapshot1)
      compacted.containsMatchingEntry(term, 1) shouldBe true
    }
    "return contains matching entry after compaction 2" in {
      val compacted = (log + entry1 + entry2).compactedWith(snapshot2)
      compacted.containsMatchingEntry(term, 2) shouldBe true
    }
    "return contains matching entry after compaction 3" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot3)
      compacted.containsMatchingEntry(term, 3) shouldBe true
    }
    "return contains entry 1 after compaction 1" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot1)
      compacted.entries.size shouldBe 3
      compacted.containsEntryAt(1) shouldBe true
    }
    "return contains entry 2 after compaction 1" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot1)
      compacted.entries.size shouldBe 3
      compacted.containsEntryAt(2) shouldBe true
    }
    "return contains entry 3 after compaction 1" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot1)
      compacted.entries.size shouldBe 3
      compacted.containsEntryAt(3) shouldBe true
    }
    "return not contains entry 1 after compaction 2" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot2)
      compacted.containsEntryAt(1) shouldBe false
    }
    "return contains entry 2 after compaction 2" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot2)
      compacted.containsEntryAt(2) shouldBe true
    }
    "return contains entry 3 after compaction 2" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot2)
      compacted.containsEntryAt(3) shouldBe true
    }
    "return not contains entry 1 after compaction 3" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot3)
      compacted.containsEntryAt(1) shouldBe false
    }
    "return not contains entry 2 after compaction 3" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot3)
      compacted.containsEntryAt(2) shouldBe false
    }
    "return contains entry 2 after compaction 3" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot3)
      compacted.containsEntryAt(3) shouldBe true
    }
    "return last term after compaction 1" in {
      val compacted = (log + entry1).compactedWith(snapshot1)
      compacted.lastTerm shouldEqual Some(term)
    }
    "return last term after compaction 2" in {
      val compacted = (log + entry1 + entry2).compactedWith(snapshot2)
      compacted.lastTerm shouldEqual Some(term)
    }
    "return last term after compaction 3" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot3)
      compacted.lastTerm shouldEqual Some(term)
    }
    "return last index after compaction 1" in {
      val compacted = (log + entry1).compactedWith(snapshot1)
      compacted.lastIndex shouldEqual 1
    }
    "return last index after compaction 2" in {
      val compacted = (log + entry1 + entry2).compactedWith(snapshot2)
      compacted.lastIndex shouldEqual 2
    }
    "return last index after compaction 3" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot3)
      compacted.lastIndex shouldEqual 3
    }
    "return contains entry after compaction 1" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot1)
      compacted.containsEntryAt(1) shouldBe true
      compacted.containsEntryAt(2) shouldBe true
      compacted.containsEntryAt(3) shouldBe true
    }
    "return contains entry after compaction 2" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot2)
      compacted.containsEntryAt(1) shouldBe false
      compacted.containsEntryAt(2) shouldBe true
      compacted.containsEntryAt(3) shouldBe true
    }
    "return contains entry after compaction 3" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot3)
      compacted.containsEntryAt(1) shouldBe false
      compacted.containsEntryAt(2) shouldBe false
      compacted.containsEntryAt(3) shouldBe true
    }
    "return committed entries after compaction 1" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot1)
      compacted.commit(3).committedEntries shouldEqual List(snapshotEntry1, entry2, entry3)
    }
    "return committed entries after compaction 2" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot2)
      compacted.commit(3).committedEntries shouldEqual List(snapshotEntry2, entry3)
    }
    "return committed entries after compaction 3" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot3)
      compacted.commit(3).committedEntries shouldEqual List(snapshotEntry3)
    }
    "return entries batch 1 from after compaction 1" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot1)
      compacted.entriesBatchFrom(1) shouldEqual List(entry2, entry3)
    }
    "return entries batch 2 from after compaction 1" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot1)
      compacted.entriesBatchFrom(2) shouldEqual List(entry3)
    }
    "return entries batch 3 from after compaction 1" in {
      val compacted = (log + entry1 + entry2 + entry3).compactedWith(snapshot1)
      compacted.entriesBatchFrom(3) shouldEqual List()
    }
  }

  "stable cluster state" should {
    val member1 = TestProbe().ref
    val member2 = TestProbe().ref
    val member3 = TestProbe().ref
    val member4 = TestProbe().ref
    val member5 = TestProbe().ref

    "return correct quorum if empty" in {
      StableClusterConfiguration(0, Set()).quorum shouldEqual 1
    }

    "return correct quorum if 1 member" in {
      StableClusterConfiguration(0, Set(member1)).quorum shouldEqual 1
    }

    "return correct quorum if 2 members" in {
      StableClusterConfiguration(0, Set(member1, member2)).quorum shouldEqual 2
    }

    "return correct quorum if 3 members" in {
      StableClusterConfiguration(0, Set(member1, member2, member3)).quorum shouldEqual 2
    }

    "return correct quorum if 4 members" in {
      StableClusterConfiguration(0, Set(member1, member2, member3, member4)).quorum shouldEqual 3
    }

    "return correct quorum if 5 members" in {
      StableClusterConfiguration(0, Set(member1, member2, member3, member4, member5)).quorum shouldEqual 3
    }
  }

  "log index" should {
    val member1 = TestProbe().ref
    val member2 = TestProbe().ref
    val member3 = TestProbe().ref
    val member4 = TestProbe().ref

    "return none index for non indexed actor" in {
      val logIndex = LogIndexMap.empty
      logIndex.indexFor(member1) shouldEqual None
    }

    "put index" in {
      val logIndex = LogIndexMap.empty
      logIndex.put(member1, 0)
      logIndex.indexFor(member1) shouldEqual Some(0)
    }

    "put index greater than current" in {
      val logIndex = LogIndexMap.empty
      logIndex.put(member1, 0)
      logIndex.putIfGreater(member1, 1) shouldEqual 1
    }

    "put index greater not than current" in {
      val logIndex = LogIndexMap.empty
      logIndex.put(member1, 1)
      logIndex.putIfGreater(member1, 0) shouldEqual 1
    }

    val commitTable = Table(
      ("members", "index", "commit index"),
      (Set.empty[ActorRef], Map.empty[ActorRef, Long], None),
      (Set(member1), Map(member1 → 1l), Some(1)),
      (Set(member1, member2), Map(member1 → 1l), None),
      (Set(member1, member2), Map(member1 → 1l, member2 → 1l), Some(1)),
      (Set(member1, member2), Map(member1 → 2l, member2 → 1l), Some(1)),
      (Set(member1, member2), Map(member1 → 2l, member2 → 2l), Some(2)),
      (Set(member1, member2), Map(member1 → 1l, member2 → 1l, member3 → 2l), Some(1)),
      (Set(member1, member2), Map(member1 → 2l, member2 → 1l, member3 → 2l), Some(1)),
      (Set(member1, member2), Map(member1 → 2l, member2 → 2l, member3 → 2l), Some(2)),
      (Set(member1, member2, member3), Map(member1 → 1l, member2 → 1l), Some(1)),
      (Set(member1, member2, member3), Map(member1 → 2l, member2 → 1l), Some(1)),
      (Set(member1, member2, member3), Map(member1 → 2l, member2 → 2l), Some(2)),
      (Set(member1, member2, member3), Map(member1 → 1l, member2 → 1l, member3 → 1l), Some(1)),
      (Set(member1, member2, member3), Map(member1 → 2l, member2 → 1l, member3 → 1l), Some(1)),
      (Set(member1, member2, member3), Map(member1 → 2l, member2 → 2l, member3 → 1l), Some(2)),
      (Set(member1, member2, member3), Map(member1 → 2l, member2 → 2l, member3 → 2l), Some(2)),
      (Set(member1, member2, member3, member4), Map(), None),
      (Set(member1, member2, member3, member4), Map(member1 → 1l), None),
      (Set(member1, member2, member3, member4), Map(member1 → 1l, member2 → 1l), None),
      (Set(member1, member2, member3, member4), Map(member1 → 1l, member2 → 1l, member3 → 1l), Some(1)),
      (Set(member1, member2, member3, member4), Map(member1 → 1l, member2 → 1l, member3 → 1l, member4 → 1l), Some(1)),
      (Set(member1, member2, member3, member4), Map(member1 → 2l, member2 → 1l, member3 → 1l, member4 → 1l), Some(1)),
      (Set(member1, member2, member3, member4), Map(member1 → 2l, member2 → 2l, member3 → 1l, member4 → 1l), Some(1)),
      (Set(member1, member2, member3, member4), Map(member1 → 2l, member2 → 2l, member3 → 2l, member4 → 1l), Some(2)),
      (Set(member1, member2, member3, member4), Map(member1 → 2l, member2 → 2l, member3 → 2l, member4 → 2l), Some(2))
    )

    forAll(commitTable) {
      (members, index, expectedCommit) ⇒
        s"check commit index: ${members.map(_.path.name)}, ${index.map((t: (ActorRef, Long)) ⇒ t._1.path.name → t._2)}, $expectedCommit" in {
          val cluster = StableClusterConfiguration(0, members)
          val logIndex = LogIndexMap.empty
          index.foreach((t: (ActorRef, Long)) ⇒ logIndex.put(t._1, t._2.toInt))
          logIndex.consensusForIndex(cluster) shouldBe expectedCommit
        }

    }
  }
}