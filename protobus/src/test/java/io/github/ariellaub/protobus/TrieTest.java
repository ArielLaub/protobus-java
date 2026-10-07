package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class TrieTest {
    private static List<String> match(Trie<String> t, String topic) {
        return t.match(topic);
    }

    @Test
    void exactWordsMatch() {
        Trie<String> t = new Trie<>();
        t.add("EVENT.a.b", "x");
        assertEquals(List.of("x"), match(t, "EVENT.a.b"));
        assertEquals(List.of(), match(t, "EVENT.a"));
        assertEquals(List.of(), match(t, "EVENT.a.b.c"));
    }

    @Test
    void starIsExactlyOneWord() {
        Trie<String> t = new Trie<>();
        t.add("EVENT.*.b", "x");
        assertEquals(List.of("x"), match(t, "EVENT.a.b"));
        assertEquals(List.of(), match(t, "EVENT.b"));
        assertEquals(List.of(), match(t, "EVENT.a.c.b"));
    }

    @Test
    void hashIsZeroOrMoreWords() {
        Trie<String> t = new Trie<>();
        t.add("EVENT.#", "x");
        assertEquals(List.of("x"), match(t, "EVENT"));
        assertEquals(List.of("x"), match(t, "EVENT.a"));
        assertEquals(List.of("x"), match(t, "EVENT.a.b.c"));
        Trie<String> mid = new Trie<>();
        mid.add("a.#.c", "y");
        assertEquals(List.of("y"), match(mid, "a.c"));
        assertEquals(List.of("y"), match(mid, "a.b.c"));
        assertEquals(List.of("y"), match(mid, "a.b.b.c"));
        assertEquals(List.of(), match(mid, "a.b"));
        Trie<String> all = new Trie<>();
        all.add("#", "z");
        assertEquals(List.of("z"), match(all, "anything.at.all"));
    }

    @Test
    void aShorterPatternStillMatchesBesideALongerOne() {
        Trie<String> t = new Trie<>();
        t.add("a.b", "short");
        t.add("a.b.c", "long");
        assertEquals(List.of("short"), match(t, "a.b"));
        assertEquals(List.of("long"), match(t, "a.b.c"));
        // A node that is only a step along a longer pattern does not match.
        assertEquals(List.of(), match(t, "a"));
    }

    @Test
    void severalValuesOnOnePatternAllMatchOnce() {
        Trie<String> t = new Trie<>();
        t.add("EVENT.x", "first");
        t.add("EVENT.x", "second");
        t.add("EVENT.*", "star");
        t.add("#", "hash");
        assertEquals(List.of("first", "second", "star", "hash"), match(t, "EVENT.x"));
    }
}
