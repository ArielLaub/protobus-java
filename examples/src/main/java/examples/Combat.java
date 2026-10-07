package examples;

import combat.GameOver;
import combat.GetStatusRequest;
import combat.GetStatusResponse;
import combat.InitiateGameRequest;
import combat.InitiateGameResponse;
import combat.PlayerDied;
import combat.PlayerJoined;
import combat.PlayerProtobus;
import combat.PlayerShot;
import combat.ShootRequest;
import combat.ShootResponse;
import combat.TurnComplete;
import io.github.ariellaub.protobus.CallContext;
import io.github.ariellaub.protobus.CallOptions;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.EventListener;
import io.github.ariellaub.protobus.LogLevel;
import io.github.ariellaub.protobus.Logger;
import io.github.ariellaub.protobus.MessageServiceOptions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * A battle royale played over protobus: six players, each its own instance of the
 * Combat.Player service (Combat.Player.player1 ... player6) with its own strategy.
 * Players shoot each other by RPC; joins, hits, deaths, turns and the winner
 * travel as events every player subscribes to.
 *
 * <pre>
 *   docker compose up -d --wait
 *   AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ ./gradlew :examples:runCombat
 * </pre>
 *
 * A port of the TypeScript, Python, Go and C++ combat samples, on the same schema:
 * it plays against their players unchanged.
 */
public final class Combat {
    private Combat() {}

    static final int STARTING_HEALTH = 10;

    static synchronized void say(String line) {
        System.out.println(line);
    }

    static final class Opponent {
        final String id;
        final String name;
        int health = STARTING_HEALTH;
        boolean alive = true;

        Opponent(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    /** What a strategy may read and keep. */
    static final class View {
        int health = STARTING_HEALTH;
        String lastAttacker = "";
        String focus = ""; // strategies that hold a grudge store it here
        final Random rand;

        View(long seed) {
            rand = new Random(seed);
        }
    }

    interface Strategy extends BiFunction<View, List<Opponent>, Opponent> {}

    static Opponent random(View v, List<Opponent> alive) {
        return alive.isEmpty() ? null : alive.get(v.rand.nextInt(alive.size()));
    }

    /** The six strategies of the original sample. */
    static final Map<String, Strategy> STRATEGIES = new LinkedHashMap<>();

    static {
        STRATEGIES.put("The Vindicator", (v, alive) -> alive.stream().filter(o -> o.id.equals(v.lastAttacker))
                .findFirst().orElseGet(() -> random(v, alive)));
        STRATEGIES.put("The Bully Hunter", (v, alive) -> alive.stream()
                .min(Comparator.comparingInt(o -> o.health)).orElse(null));
        STRATEGIES.put("The Giant Slayer", (v, alive) -> alive.stream()
                .max(Comparator.comparingInt(o -> o.health)).orElse(null));
        STRATEGIES.put("The Equalizer", (v, alive) -> alive.stream()
                .min(Comparator.comparingInt(o -> Math.abs(o.health - v.health))).orElse(null));
        STRATEGIES.put("The Wildcard", Combat::random);
        STRATEGIES.put("The Terminator", (v, alive) -> {
            for (Opponent o : alive) if (o.id.equals(v.focus)) return o;
            Opponent o = random(v, alive);
            if (o != null) v.focus = o.id;
            return o;
        });
    }

    /**
     * One contestant: an instance of Combat.Player. Requests and events are separate
     * consumers, so handlers run concurrently and the state is guarded.
     */
    static final class Player extends PlayerProtobus.Base {
        final String id;
        final String name;
        private final Strategy strategy;
        private final View view;
        private final Map<String, Opponent> others = new LinkedHashMap<>();
        private List<String> order = List.of();
        private boolean gameOver;

        Player(Context ctx, String id, String name, Strategy strategy, long seed) {
            super(ctx);
            this.id = id;
            this.name = name;
            this.strategy = strategy;
            this.view = new View(seed);
        }

        /** Each player is an instance of the one Combat.Player contract. */
        @Override
        public String serviceName() {
            return "Combat.Player." + id;
        }

        @Override
        public ShootResponse shoot(ShootRequest request, CallContext context) {
            boolean hit;
            int health;
            String shooter = request.getShooterId();
            synchronized (this) {
                if (view.health <= 0) return ShootResponse.newBuilder().setHit(false).build();
                hit = view.rand.nextBoolean();
                if (hit) {
                    view.health--;
                    view.lastAttacker = request.getShooterId();
                }
                health = view.health;
                Opponent o = others.get(shooter);
                if (o != null) shooter = o.name;
            }
            say(hit ? "  " + name + " was hit by " + shooter + "! Health: " + health
                    : "  " + name + " dodged an attack from " + shooter + "!");
            publishEvent(PlayerShot.newBuilder().setShooterId(request.getShooterId()).setTargetId(id).setHit(hit)
                    .setTargetHealth(health).build());
            if (hit && health <= 0) {
                say("  " + name + " has been eliminated!");
                publishEvent(PlayerDied.newBuilder().setPlayerId(id).setKilledBy(request.getShooterId()).build());
            }
            return ShootResponse.newBuilder().setHit(hit).setRemainingHealth(health).build();
        }

        /** The turn order. The first player takes its turn on a thread of its own, so the RPC answers at once. */
        @Override
        public InitiateGameResponse initiateGame(InitiateGameRequest request, CallContext context) {
            synchronized (this) {
                order = List.copyOf(request.getPlayerOrderList());
            }
            if (request.getMyIndex() == 0) new Thread(this::takeTurn).start();
            return InitiateGameResponse.newBuilder().setSuccess(true).build();
        }

        @Override
        public synchronized GetStatusResponse getStatus(GetStatusRequest request, CallContext context) {
            return GetStatusResponse.newBuilder().setPlayerId(id).setPlayerName(name).setHealth(view.health)
                    .setAlive(view.health > 0).build();
        }

        synchronized void meet(String otherId, String otherName) {
            if (!otherId.equals(id)) others.putIfAbsent(otherId, new Opponent(otherId, otherName));
        }

        void subscribe() {
            subscribeEvent(PlayerJoined.class, (e, t, x) -> meet(e.getPlayerId(), e.getPlayerName()));
            subscribeEvent(PlayerShot.class, (e, t, x) -> {
                synchronized (this) {
                    Opponent o = others.get(e.getTargetId());
                    if (o != null) {
                        o.health = e.getTargetHealth();
                        o.alive = e.getTargetHealth() > 0;
                    }
                }
            });
            subscribeEvent(PlayerDied.class, (e, t, x) -> {
                synchronized (this) {
                    Opponent o = others.get(e.getPlayerId());
                    if (o != null) {
                        o.health = 0;
                        o.alive = false;
                    }
                    if (view.focus.equals(e.getPlayerId())) view.focus = "";
                }
            });
            subscribeEvent(TurnComplete.class, (e, t, x) -> {
                boolean mine;
                synchronized (this) {
                    mine = order.indexOf(id) == e.getNextPlayerIndex();
                }
                if (mine) takeTurn();
            });
            subscribeEvent(GameOver.class, (e, t, x) -> {
                synchronized (this) {
                    gameOver = true;
                }
            });
        }

        private List<Opponent> aliveOthers() {
            List<Opponent> alive = new ArrayList<>();
            for (Opponent o : others.values()) if (o.alive) alive.add(o);
            return alive;
        }

        private void takeTurn() {
            Opponent target = null;
            synchronized (this) {
                if (gameOver) return;
                if (view.health > 0) {
                    List<Opponent> alive = aliveOthers();
                    if (alive.isEmpty()) {
                        win();
                        return;
                    }
                    target = strategy.apply(view, alive);
                }
            }
            // A turn handed to a player who died meanwhile is passed on, not
            // dropped: dropping it would stall the game.
            if (target != null) {
                say("  " + name + " shoots at " + target.name + "!");
                PlayerProtobus.Proxy victim = new PlayerProtobus.Proxy(context(), "Combat.Player." + target.id);
                victim.init();
                try {
                    ShootResponse result = victim.shoot(ShootRequest.newBuilder().setShooterId(id).build(),
                            CallOptions.DEFAULT.withActor(id));
                    if (result.getRemainingHealth() <= 0) {
                        synchronized (this) {
                            target.alive = false;
                            target.health = 0;
                        }
                    }
                } catch (RuntimeException e) {
                    say("  " + name + " failed to shoot: " + e.getMessage());
                }
                synchronized (this) {
                    if (aliveOthers().isEmpty()) {
                        win();
                        return;
                    }
                }
            }
            endTurn();
        }

        private void win() {
            say("  " + name + " is the last one standing!");
            publishEvent(GameOver.newBuilder().setWinnerId(id).setWinnerName(name).build());
        }

        private void endTurn() {
            int next;
            synchronized (this) {
                int me = order.indexOf(id);
                int n = order.size();
                next = (me + 1) % n;
                for (int i = 1; i <= n; i++) {
                    int idx = (me + i) % n;
                    Opponent o = others.get(order.get(idx));
                    if (o != null && o.alive) {
                        next = idx;
                        break;
                    }
                }
            }
            publishEvent(TurnComplete.newBuilder().setPlayerId(id).setNextPlayerIndex(next).build());
        }
    }

    public static void main(String[] args) throws Exception {
        // The game narrates itself; the framework's own lines would drown it out.
        Logger.setLevel(LogLevel.WARN);
        String url = System.getenv().getOrDefault("AMQP_URL", "amqp://guest:guest@127.0.0.1:25672/");
        try (Context context = new Context()) {
            context.init(url);
            String rule = "=".repeat(60);
            say(rule + "\nCOMBAT GAME - Battle Royale!\n" + rule);

            // Hear the result like any other subscriber.
            EventListener results = new EventListener(context.connection(), context.factory(), null);
            results.init(null, "");
            CompletableFuture<String> winner = new CompletableFuture<>();
            results.subscribe(GameOver.getDefaultInstance(), (e, t, x) -> winner.complete(e.getWinnerName()), null);
            results.start();

            long seed = System.currentTimeMillis();
            List<Player> players = new ArrayList<>();
            List<String> order = new ArrayList<>();
            int i = 0;
            for (Map.Entry<String, Strategy> s : STRATEGIES.entrySet()) {
                String id = "player" + (++i);
                Player p = new Player(context, id, s.getKey(), s.getValue(), seed + i);
                p.init();
                p.subscribe();
                players.add(p);
                order.add(id);
                say("  joined: " + p.name + " (" + id + ")");
            }
            for (Player p : players) {
                for (Player o : players) p.meet(o.id, o.name);
                context.publishEvent(PlayerJoined.newBuilder().setPlayerId(p.id).setPlayerName(p.name)
                        .setHealth(STARTING_HEALTH).build());
            }
            say("Turn order: " + String.join(" -> ", order) + "\n" + rule + "\nLET THE BATTLE BEGIN!\n" + rule);

            // Index 0 is initiated last: its first turn passes the turn on, and every
            // other player must know the order by then.
            for (int idx = order.size() - 1; idx >= 0; idx--) {
                PlayerProtobus.Proxy player = new PlayerProtobus.Proxy(context, "Combat.Player." + order.get(idx));
                player.init();
                player.initiateGame(InitiateGameRequest.newBuilder().addAllPlayerOrder(order).setMyIndex(idx).build());
            }

            try {
                winner.get(2, TimeUnit.MINUTES);
            } catch (java.util.concurrent.TimeoutException e) {
                System.err.println("the game did not finish");
                System.exit(1);
            }

            say(rule + "\nFINAL RESULTS\n" + rule);
            for (String id : order) {
                PlayerProtobus.Proxy player = new PlayerProtobus.Proxy(context, "Combat.Player." + id);
                player.init();
                GetStatusResponse status = player.getStatus(GetStatusRequest.getDefaultInstance());
                say(String.format("  %-18s%2d HP  (%s)", status.getPlayerName(), status.getHealth(),
                        status.getAlive() ? "WINNER" : "eliminated"));
            }
            for (Player p : players) p.stopConsuming();
            context.connection().drainInFlight(5000);
        }
    }
}
