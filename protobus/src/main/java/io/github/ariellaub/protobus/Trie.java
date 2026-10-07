package io.github.ariellaub.protobus;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Matches dotted topics against AMQP topic patterns, as the broker does:
 * {@code *} stands for exactly one word and {@code #} for zero or more.
 *
 * Several values may be registered under one pattern; {@link #match} returns each
 * matching value once, in registration order. Not thread-safe: the event listener
 * guards it.
 */
public final class Trie<T> {
    private final Node<T> root = new Node<>("");

    public void add(String pattern, T value) {
        Node<T> node = root;
        for (String word : pattern.split("\\.", -1)) {
            node = node.children.computeIfAbsent(word, Node::new);
        }
        // Only the node the pattern ends on carries the value: marking every node
        // along the path would make each one look like a registered pattern.
        node.values.add(value);
    }

    public List<T> match(String topic) {
        String[] words = topic.split("\\.", -1);
        Set<Node<T>> ends = new LinkedHashSet<>();
        for (Node<T> child : root.children.values()) collect(child, words, 0, ends);
        Set<T> results = new LinkedHashSet<>();
        for (Node<T> node : ends) results.addAll(node.values);
        return new ArrayList<>(results);
    }

    /** Every node at which a pattern ending there matches words[i..]. {@code node} consumes from words[i]. */
    private static <T> void collect(Node<T> node, String[] words, int i, Set<Node<T>> out) {
        if (node.superWildcard) {
            // '#' consumes zero words: hand words[i..] straight to the children...
            if (i == words.length) {
                if (!node.values.isEmpty()) out.add(node);
            }
            for (Node<T> child : node.children.values()) collect(child, words, i, out);
            // ...or one more word, staying on '#'.
            if (i < words.length) collect(node, words, i + 1, out);
            return;
        }
        if (i >= words.length) return;
        if (!node.wildcard && !node.word.equals(words[i])) return;
        int next = i + 1;
        if (next == words.length) {
            // A pattern ends here if anything was registered on this node,
            // independently of whether longer patterns branch off it.
            if (!node.values.isEmpty()) out.add(node);
            Node<T> hash = node.children.get("#");
            if (hash != null) collect(hash, words, next, out);
            return;
        }
        for (Node<T> child : node.children.values()) collect(child, words, next, out);
    }

    private static final class Node<T> {
        final String word;
        final boolean wildcard;
        final boolean superWildcard;
        final List<T> values = new ArrayList<>();
        final Map<String, Node<T>> children = new LinkedHashMap<>();

        Node(String word) {
            this.word = word;
            this.wildcard = word.equals("*") || word.equals("#");
            this.superWildcard = word.equals("#");
        }
    }
}
