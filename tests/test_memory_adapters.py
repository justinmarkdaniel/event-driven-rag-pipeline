from rag_ingest.adapters.memory import InMemoryRegistry, LocalQueue
from rag_ingest.domain import ClaimResult, DocumentRef


class TestRegistry:
    def test_only_one_claim_wins(self) -> None:
        registry = InMemoryRegistry(clock=lambda: 0)
        assert registry.claim("h", "a", 60) is ClaimResult.CLAIMED
        assert registry.claim("h", "b", 60) is ClaimResult.IN_PROGRESS

    def test_expired_lease_can_be_taken_over(self) -> None:
        now = [0.0]
        registry = InMemoryRegistry(clock=lambda: now[0])
        registry.claim("h", "a", 60)
        now[0] = 61
        assert registry.claim("h", "b", 60) is ClaimResult.CLAIMED

    def test_completed_content_reports_already_indexed(self) -> None:
        registry = InMemoryRegistry()
        registry.claim("h", "a", 60)
        registry.complete("h")
        assert registry.claim("h", "b", 60) is ClaimResult.ALREADY_INDEXED

    def test_release_does_not_undo_completion(self) -> None:
        registry = InMemoryRegistry()
        registry.claim("h", "a", 60)
        registry.complete("h")
        registry.release("h")
        assert registry.status("h") == "COMPLETE"


class TestLocalQueue:
    def test_message_moves_to_dlq_after_max_receives(self) -> None:
        queue = LocalQueue(max_receive_count=3)
        queue.send(DocumentRef("b", "k"))

        receives = 0
        while (message := queue.receive()) is not None:
            receives += 1
            queue.nack(message)

        assert receives == 3
        assert [m.ref.key for m in queue.dead_letters] == ["k"]
