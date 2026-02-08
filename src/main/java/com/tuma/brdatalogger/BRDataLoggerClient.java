package com.tuma.brdatalogger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongIterator;
import net.fabricmc.api.ClientModInitializer;
import net.fabricmc.fabric.api.client.command.v2.ClientCommandRegistrationCallback;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayConnectionEvents;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientTickEvents;
import net.fabricmc.loader.api.FabricLoader;
import net.minecraft.client.MinecraftClient;
import net.minecraft.client.network.ClientPlayerEntity;
import net.minecraft.client.world.ClientWorld;
import net.minecraft.text.Text;
import net.minecraft.util.math.BlockPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mojang.brigadier.arguments.StringArgumentType.getString;
import static com.mojang.brigadier.arguments.StringArgumentType.greedyString;
import static net.fabricmc.fabric.api.client.command.v2.ClientCommandManager.argument;
import static net.fabricmc.fabric.api.client.command.v2.ClientCommandManager.literal;

public class BRDataLoggerClient implements ClientModInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger("BRBaseLogger");
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final Gson PRETTY_GSON = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ISO_INSTANT;
    private static final String DATA_DIR_NAME = "bedrockrooms-base-logger-data";
    private static final int TICK_INTERVAL = 20;
    private static final long MIN_WRITE_INTERVAL_MS = 30_000L;
    private static final Pattern SERVER_ID_PATTERN = Pattern.compile("#\\s*(\\d+)");
    private static Method cachedSidebarMethod;
    private static Method cachedScoresMethod;
    private static volatile List<String> lastHudLines = List.of();
    private static volatile String lastHudTitle;
    private static volatile long lastHudUpdateMs;

    private static final BrAccess BR = new BrAccess();
    private static final Map<Long, RoomWriteState> ROOM_STATES = new HashMap<>();

    private static LoggerConfig config;
    private static String activeProfile;
    private static boolean warnedNoProfile;
    private static int tickCounter;

    @Override
    public void onInitializeClient() {
        loadConfig();
        activeProfile = config.profile;

        ClientCommandRegistrationCallback.EVENT.register((dispatcher, registryAccess) -> {
            dispatcher.register(literal("brlog")
                    .then(literal("server")
                            .then(argument("name", greedyString())
                                    .executes(ctx -> {
                                        String name = getString(ctx, "name");
                                        setProfile(ctx.getSource().getClient(), name);
                                        return 1;
                                    })))
                    .then(literal("status")
                            .executes(ctx -> {
                                String msg = activeProfile == null || activeProfile.isBlank()
                                        ? "[BR-LOG] No profile selected."
                                        : "[BR-LOG] Active profile: " + activeProfile;
                                ctx.getSource().sendFeedback(Text.literal(msg));
                                return 1;
                            }))
                    .then(literal("debug")
                            .executes(ctx -> {
                                MinecraftClient client = ctx.getSource().getClient();
                                String detected = detectProfileFromScoreboard(client);
                                ctx.getSource().sendFeedback(Text.literal("[BR-LOG] Detected: " + (detected == null ? "-" : detected)));
                                String title = getScoreboardTitle(client);
                                if (title != null && !title.isBlank()) {
                                    ctx.getSource().sendFeedback(Text.literal("[BR-LOG] Title: " + title));
                                }
                                List<String> lines = getScoreboardDebugLines(client);
                                if (lines.isEmpty()) {
                                    ctx.getSource().sendFeedback(Text.literal("[BR-LOG] Scoreboard lines: <none>"));
                                } else {
                                    ctx.getSource().sendFeedback(Text.literal("[BR-LOG] Scoreboard lines:"));
                                    for (String line : lines) {
                                        ctx.getSource().sendFeedback(Text.literal("  " + line));
                                    }
                                }
                                return 1;
                            }))
                    .then(literal("dump")
                            .executes(ctx -> {
                                MinecraftClient client = ctx.getSource().getClient();
                                logRooms(client, true);
                                ctx.getSource().sendFeedback(Text.literal("[BR-LOG] Manual dump complete."));
                                return 1;
                            }))
            );
        });

        ClientTickEvents.END_CLIENT_TICK.register(client -> {
            if (client == null || client.world == null || client.player == null) {
                return;
            }
            tickCounter++;
            if (tickCounter % TICK_INTERVAL != 0) {
                return;
            }
            updateProfileFromScoreboard(client);
            logRooms(client, false);
        });

        ClientPlayConnectionEvents.JOIN.register((handler, sender, client) -> {
            ROOM_STATES.clear();
            warnedNoProfile = false;
            tickCounter = 0;
        });

        ClientPlayConnectionEvents.DISCONNECT.register((handler, client) -> {
            ROOM_STATES.clear();
            warnedNoProfile = false;
            tickCounter = 0;
        });
    }

    private static void setProfile(MinecraftClient client, String rawName) {
        String name = sanitizeProfileName(rawName);
        if (name.isEmpty()) {
            if (client != null && client.player != null) {
                client.player.sendMessage(Text.literal("[BR-LOG] Invalid profile name."), false);
            }
            return;
        }
        activeProfile = name;
        config.profile = name;
        saveConfig();
        warnedNoProfile = false;
        if (client != null && client.player != null) {
            client.player.sendMessage(Text.literal("[BR-LOG] Profile set to: " + name), false);
            client.player.sendMessage(Text.literal("[BR-LOG] Data folder: " + getProfileDir(name)), false);
        }
    }

    private static void logRooms(MinecraftClient client, boolean forceDump) {
        if (!BR.isAvailable()) {
            return;
        }
        if (activeProfile == null || activeProfile.isBlank()) {
            if (!warnedNoProfile && client != null && client.player != null) {
                client.player.sendMessage(Text.literal("[BR-LOG] Set a profile first: /brlog server <name>"), false);
                warnedNoProfile = true;
            }
            return;
        }
        Iterable<?> rooms = BR.getRoomsIterable();
        if (rooms == null) {
            return;
        }
        ClientWorld world = client.world;
        int minBlast = BR.getMinBlastSafeCellsToShow();
        int minIdeal = BR.getMinIdealPrivateCellsToShow();
        int yMax = BR.getYMax();
        int pad = getBrymaxPad();
        if (pad < 0) {
            pad = 2;
        }

        for (Object room : rooms) {
            RoomSnapshot snapshot = snapshotRoom(room, world, yMax, pad, minBlast, minIdeal);
            if (snapshot == null) {
                continue;
            }
            if (!shouldWriteSnapshot(snapshot, forceDump)) {
                continue;
            }
            writeSnapshot(snapshot);
        }
    }

    private static void updateProfileFromScoreboard(MinecraftClient client) {
        if (client == null || client.player == null || client.world == null) {
            return;
        }
        String detected = detectProfileFromScoreboard(client);
        if (detected == null || detected.isBlank()) {
            return;
        }
        if (!detected.equals(activeProfile)) {
            activeProfile = detected;
            config.profile = detected;
            saveConfig();
            warnedNoProfile = false;
            client.player.sendMessage(Text.literal("[BR-LOG] Auto profile: " + detected), false);
        }
    }

    private static String detectProfileFromScoreboard(MinecraftClient client) {
        net.minecraft.scoreboard.Scoreboard scoreboard = getClientScoreboard(client);
        Object objective = getSidebarObjective(scoreboard);
        if (objective != null) {
            List<String> lines = gatherScoreboardLines(client, scoreboard, objective);
            String found = detectProfileFromLines(lines);
            if (found != null) {
                return found;
            }
        }
        return detectProfileFromLines(getCachedHudLines());
    }

    private static String detectProfileFromLines(List<String> lines) {
        if (lines == null || lines.isEmpty()) {
            return null;
        }
        for (String line : lines) {
            if (line == null || line.isBlank()) {
                continue;
            }
            String clean = stripColor(line).toLowerCase(Locale.ROOT);
            Matcher m = SERVER_ID_PATTERN.matcher(clean);
            if (!m.find()) {
                continue;
            }
            String id = m.group(1);
            if (clean.contains("классик")) {
                return "classic-" + id;
            }
            if (clean.contains("лайт")) {
                return "lite-" + id;
            }
        }
        return null;
    }

    private static List<String> getScoreboardDebugLines(MinecraftClient client) {
        if (client == null || client.world == null) {
            return List.of();
        }
        net.minecraft.scoreboard.Scoreboard scoreboard = getClientScoreboard(client);
        Object objective = getSidebarObjective(scoreboard);
        if (objective == null) {
            return List.of();
        }
        return gatherScoreboardLines(client, scoreboard, objective);
    }

    private static List<String> gatherScoreboardLines(MinecraftClient client, net.minecraft.scoreboard.Scoreboard scoreboard, Object objective) {
        List<String> lines = new ArrayList<>();
        String title = readObjectiveTitle(objective);
        if (title != null && !title.isBlank()) {
            lines.add(title);
        }
        lines.addAll(readScoreboardLines(scoreboard, objective));
        return lines;
    }

    private static Object getSidebarObjective(net.minecraft.scoreboard.Scoreboard scoreboard) {
        if (scoreboard == null) {
            return null;
        }
        if (cachedSidebarMethod != null) {
            Object objective = invokeSidebarMethod(scoreboard, cachedSidebarMethod);
            if (objective != null) {
                return objective;
            }
        }
        try {
            Method m = scoreboard.getClass().getMethod("getObjectiveForSlot", int.class);
            cachedSidebarMethod = m;
            Object objective = invokeSidebarMethod(scoreboard, m);
            if (objective != null) {
                return objective;
            }
        } catch (Throwable t) {
            // ignore and fallback below
        }
        try {
            Class<?> slotClass = Class.forName("net.minecraft.scoreboard.ScoreboardDisplaySlot");
            Object sidebarSlot = null;
            for (Object constant : slotClass.getEnumConstants()) {
                if (constant != null && constant.toString().equalsIgnoreCase("SIDEBAR")) {
                    sidebarSlot = constant;
                    break;
                }
            }
            if (sidebarSlot == null) {
                return null;
            }
            for (Method m : scoreboard.getClass().getMethods()) {
                Class<?>[] params = m.getParameterTypes();
                if (params.length == 1 && params[0] == slotClass) {
                    cachedSidebarMethod = m;
                    Object objective = invokeSidebarMethod(scoreboard, m, sidebarSlot);
                    if (objective != null) {
                        return objective;
                    }
                }
            }
        } catch (Throwable t) {
            LOGGER.debug("Failed to read scoreboard objective.", t);
        }
        return null;
    }

    private static String readObjectiveTitle(Object objective) {
        try {
            Method m = objective.getClass().getMethod("getDisplayName");
            Object text = m.invoke(objective);
            return readTextString(text);
        } catch (Throwable t) {
            return null;
        }
    }

    private static String getScoreboardTitle(MinecraftClient client) {
        if (client == null || client.world == null) {
            return null;
        }
        net.minecraft.scoreboard.Scoreboard scoreboard = getClientScoreboard(client);
        Object objective = getSidebarObjective(scoreboard);
        if (objective == null) {
            return null;
        }
        return readObjectiveTitle(objective);
    }

    private static List<String> readScoreboardLines(net.minecraft.scoreboard.Scoreboard scoreboard, Object objective) {
        List<String> lines = new ArrayList<>();
        Collection<?> scores = getScoresCollection(scoreboard, objective);
        if (scores == null) {
            return lines;
        }
        for (Object score : scores) {
            String name = readScoreName(score);
            if (name != null && !name.isBlank()) {
                String line = decorateScoreLine(scoreboard, name);
                lines.add(line == null ? name : line);
            }
        }
        return lines;
    }

    private static Collection<?> getScoresCollection(net.minecraft.scoreboard.Scoreboard scoreboard, Object objective) {
        if (scoreboard == null || objective == null) {
            return null;
        }
        if (cachedScoresMethod != null) {
            try {
                Object result = cachedScoresMethod.invoke(scoreboard, objective);
                if (result instanceof Collection) {
                    return (Collection<?>) result;
                }
            } catch (Throwable t) {
                cachedScoresMethod = null;
            }
        }
        Method direct = findScoresMethod(scoreboard, objective.getClass());
        if (direct != null) {
            cachedScoresMethod = direct;
            try {
                Object result = direct.invoke(scoreboard, objective);
                if (result instanceof Collection) {
                    return (Collection<?>) result;
                }
            } catch (Throwable t) {
                cachedScoresMethod = null;
            }
        }
        return null;
    }

    private static Method findScoresMethod(net.minecraft.scoreboard.Scoreboard scoreboard, Class<?> objectiveClass) {
        Method fallback = null;
        for (Method m : scoreboard.getClass().getMethods()) {
            Class<?>[] params = m.getParameterTypes();
            if (params.length != 1 || !params[0].isAssignableFrom(objectiveClass)) {
                continue;
            }
            Class<?> ret = m.getReturnType();
            if (Collection.class.isAssignableFrom(ret)) {
                return m;
            }
            if (Iterable.class.isAssignableFrom(ret)) {
                fallback = m;
            }
        }
        return fallback;
    }

    private static List<String> readScoreboardHudLines(MinecraftClient client, Object objective) {
        if (client == null || client.inGameHud == null || objective == null) {
            return List.of();
        }
        Object hud = null;
        try {
            Method m = client.inGameHud.getClass().getMethod("getScoreboardHud");
            hud = m.invoke(client.inGameHud);
        } catch (Throwable t) {
            hud = null;
        }
        if (hud == null) {
            return List.of();
        }
        Method method = null;
        for (Method m : hud.getClass().getDeclaredMethods()) {
            if (!m.getName().equals("getTextList")) {
                continue;
            }
            if (m.getParameterCount() != 1) {
                continue;
            }
            method = m;
            break;
        }
        if (method == null) {
            return List.of();
        }
        try {
            method.setAccessible(true);
            Object list = method.invoke(hud, objective);
            if (!(list instanceof Iterable)) {
                return List.of();
            }
            List<String> lines = new ArrayList<>();
            for (Object item : (Iterable<?>) list) {
                String text = readTextString(item);
                if (text != null && !text.isBlank()) {
                    lines.add(text);
                }
            }
            return lines;
        } catch (Throwable t) {
            return List.of();
        }
    }

    public static void updateHudSnapshot(Object title, List<?> lines) {
        List<String> textLines = new ArrayList<>();
        if (lines != null) {
            for (Object item : lines) {
                String text = readTextString(item);
                if (text != null && !text.isBlank()) {
                    textLines.add(text);
                }
            }
        }
        lastHudLines = textLines;
        lastHudTitle = readTextString(title);
        lastHudUpdateMs = System.currentTimeMillis();
    }

    private static List<String> getCachedHudLines() {
        long now = System.currentTimeMillis();
        if (now - lastHudUpdateMs > 5000L) {
            return List.of();
        }
        List<String> lines = new ArrayList<>();
        if (lastHudTitle != null && !lastHudTitle.isBlank()) {
            lines.add(lastHudTitle);
        }
        lines.addAll(lastHudLines);
        return lines;
    }

    public static net.minecraft.scoreboard.Scoreboard getClientScoreboardForHud(MinecraftClient client) {
        try {
            Method m = client.getClass().getMethod("getNetworkHandler");
            Object nh = m.invoke(client);
            if (nh != null) {
                Method m2 = nh.getClass().getMethod("getScoreboard");
                Object sb = m2.invoke(nh);
                if (sb instanceof net.minecraft.scoreboard.Scoreboard) {
                    return (net.minecraft.scoreboard.Scoreboard) sb;
                }
            }
        } catch (Throwable t) {
            // ignore, fallback below
        }
        return client.world.getScoreboard();
    }

    private static net.minecraft.scoreboard.Scoreboard getClientScoreboard(MinecraftClient client) {
        return getClientScoreboardForHud(client);
    }

    private static boolean shouldWriteSnapshot(RoomSnapshot snapshot, boolean forceDump) {
        if (snapshot == null) {
            return false;
        }
        if (!forceDump) {
            if (snapshot.yMax.loaded <= 0) {
                return false;
            }
            if (config != null && config.maxBedrockPct >= 0.0 && snapshot.yMax.loaded > 0 && snapshot.yMax.bedrockPct > config.maxBedrockPct) {
                return false;
            }
        }
        long now = System.currentTimeMillis();
        RoomWriteState state = ROOM_STATES.get(snapshot.roomId);
        String signature = snapshot.signatureKey();
        if (!forceDump && state != null) {
            if (signature.equals(state.lastSignature)) {
                return false;
            }
            if (state.lastLoaded > 0 && snapshot.yMax.loaded > 0) {
                boolean loadedIncreased = snapshot.yMax.loaded > state.lastLoaded;
                if (!loadedIncreased && now - state.lastWriteMs < MIN_WRITE_INTERVAL_MS) {
                    return false;
                }
            }
        }
        RoomWriteState next = state == null ? new RoomWriteState() : state;
        next.lastSignature = signature;
        next.lastLoaded = snapshot.yMax.loaded;
        next.lastWriteMs = now;
        ROOM_STATES.put(snapshot.roomId, next);
        return true;
    }

    private static Object invokeSidebarMethod(net.minecraft.scoreboard.Scoreboard scoreboard, Method method, Object... args) {
        try {
            if (args == null || args.length == 0) {
                return method.invoke(scoreboard, 1);
            }
            return method.invoke(scoreboard, args);
        } catch (Throwable t) {
            return null;
        }
    }

    private static String decorateScoreLine(net.minecraft.scoreboard.Scoreboard scoreboard, String name) {
        Object team = getPlayerTeam(scoreboard, name);
        if (team == null) {
            return name;
        }
        String prefix = readTextString(invokeGetter(team, "getPrefix"));
        String suffix = readTextString(invokeGetter(team, "getSuffix"));
        StringBuilder sb = new StringBuilder();
        if (prefix != null && !prefix.isBlank()) {
            sb.append(prefix);
        }
        sb.append(name);
        if (suffix != null && !suffix.isBlank()) {
            sb.append(suffix);
        }
        return sb.toString();
    }

    private static Object getPlayerTeam(net.minecraft.scoreboard.Scoreboard scoreboard, String name) {
        try {
            Method m = scoreboard.getClass().getMethod("getPlayerTeam", String.class);
            return m.invoke(scoreboard, name);
        } catch (Throwable t) {
            return null;
        }
    }

    private static String readScoreName(Object score) {
        String name = invokeStringGetter(score, "getPlayerName");
        if (name != null) {
            return name;
        }
        name = invokeStringGetter(score, "getName");
        if (name != null) {
            return name;
        }
        return invokeStringGetter(score, "getScoreHolderName");
    }

    private static Object invokeGetter(Object target, String methodName) {
        try {
            Method m = target.getClass().getMethod(methodName);
            return m.invoke(target);
        } catch (Throwable t) {
            return null;
        }
    }

    private static String invokeStringGetter(Object target, String methodName) {
        try {
            Method m = target.getClass().getMethod(methodName);
            Object value = m.invoke(target);
            return value == null ? null : value.toString();
        } catch (Throwable t) {
            return null;
        }
    }

    private static String readTextString(Object text) {
        if (text == null) {
            return null;
        }
        try {
            Method m = text.getClass().getMethod("getString");
            Object value = m.invoke(text);
            return value == null ? null : value.toString();
        } catch (Throwable t) {
            return text.toString();
        }
    }

    private static String stripColor(String value) {
        StringBuilder out = new StringBuilder(value.length());
        boolean skip = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (skip) {
                skip = false;
                continue;
            }
            if (c == '\u00a7') {
                skip = true;
                continue;
            }
            out.append(c);
        }
        return out.toString();
    }

    private static RoomSnapshot snapshotRoom(Object room, ClientWorld world, int yMax, int pad, int minBlastSafeCellsToShow, int minIdealPrivateCellsToShow) {
        if (room == null || world == null) {
            return null;
        }
        if (!BR.fillRoomFields(room)) {
            return null;
        }
        LongCollection cells = BR.roomCells;
        if (cells == null || cells.isEmpty()) {
            return null;
        }
        int blastSafeCount = BR.getBlastSafeCount(room, world);
        if (minBlastSafeCellsToShow > 0 && blastSafeCount >= 0 && blastSafeCount < minBlastSafeCellsToShow) {
            return null;
        }
        if (minIdealPrivateCellsToShow > 0 && BR.idealPrivateCount < minIdealPrivateCellsToShow) {
            return null;
        }

        Bounds bounds = computeBounds(cells);
        int roomCellsCount = cells.size();
        int exitCount = BR.exitCells == null ? 0 : BR.exitCells.size();
        int exitPercent = roomCellsCount > 0 ? (int) Math.round(exitCount * 100.0 / roomCellsCount) : 0;
        double blastSafePercent = blastSafeCount >= 0 && roomCellsCount > 0
                ? blastSafeCount * 100.0 / roomCellsCount
                : -1.0;

        YMaxSnapshot yMaxSnapshot = computeYMaxSnapshot(world, cells, yMax, pad);

        return new RoomSnapshot(
                activeProfile,
                BR.roomId,
                BR.centerX,
                BR.centerY,
                BR.centerZ,
                bounds,
                roomCellsCount,
                exitCount,
                exitPercent,
                BR.standableCount,
                BR.placeableCount,
                BR.sealed,
                BR.exitComponents,
                BR.maxExitComponentSize,
                BR.privateBestScore,
                blastSafeCount,
                blastSafePercent,
                BR.idealPrivateCount,
                yMaxSnapshot
        );
    }

    private static Bounds computeBounds(LongCollection cells) {
        int minX = Integer.MAX_VALUE;
        int minY = Integer.MAX_VALUE;
        int minZ = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int maxY = Integer.MIN_VALUE;
        int maxZ = Integer.MIN_VALUE;
        LongIterator it = cells.iterator();
        while (it.hasNext()) {
            long l = it.nextLong();
            int x = BlockPos.unpackLongX(l);
            int y = BlockPos.unpackLongY(l);
            int z = BlockPos.unpackLongZ(l);
            if (x < minX) {
                minX = x;
            }
            if (y < minY) {
                minY = y;
            }
            if (z < minZ) {
                minZ = z;
            }
            if (x > maxX) {
                maxX = x;
            }
            if (y > maxY) {
                maxY = y;
            }
            if (z > maxZ) {
                maxZ = z;
            }
        }
        return new Bounds(minX, maxX, minY, maxY, minZ, maxZ);
    }

    private static YMaxSnapshot computeYMaxSnapshot(ClientWorld world, LongCollection cells, int yMax, int pad) {
        if (yMax == Integer.MIN_VALUE || cells == null || cells.isEmpty()) {
            return new YMaxSnapshot(yMax, pad, 0, 0, 0, 0, 0, 0, 0);
        }
        int minX = Integer.MAX_VALUE;
        int minZ = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int maxZ = Integer.MIN_VALUE;
        LongIterator it = cells.iterator();
        while (it.hasNext()) {
            long l = it.nextLong();
            int x = BlockPos.unpackLongX(l);
            int z = BlockPos.unpackLongZ(l);
            if (x < minX) {
                minX = x;
            }
            if (z < minZ) {
                minZ = z;
            }
            if (x > maxX) {
                maxX = x;
            }
            if (z > maxZ) {
                maxZ = z;
            }
        }
        int x0 = minX - pad;
        int x1 = maxX + pad;
        int z0 = minZ - pad;
        int z1 = maxZ + pad;
        int width = x1 - x0 + 1;
        int depth = z1 - z0 + 1;
        int total = 0;
        int bedrock = 0;
        int air = 0;
        int other = 0;
        int skipped = 0;
        BlockPos.Mutable pos = new BlockPos.Mutable();
        for (int x = x0; x <= x1; ++x) {
            for (int z = z0; z <= z1; ++z) {
                if (!world.getChunkManager().isChunkLoaded(x >> 4, z >> 4)) {
                    skipped++;
                    continue;
                }
                total++;
                pos.set(x, yMax, z);
                net.minecraft.block.BlockState st = world.getBlockState(pos);
                if (st.isAir() || world.getFluidState(pos).isIn(net.minecraft.registry.tag.FluidTags.LAVA)) {
                    air++;
                } else if (st.isOf(net.minecraft.block.Blocks.BEDROCK)) {
                    bedrock++;
                } else {
                    other++;
                }
            }
        }
        return new YMaxSnapshot(yMax, pad, width, depth, total, skipped, bedrock, air, other);
    }

    private static void writeSnapshot(RoomSnapshot snapshot) {
        Path profileDir = getProfileDir(snapshot.profile);
        try {
            Files.createDirectories(profileDir);
        } catch (IOException e) {
            LOGGER.warn("Failed to create profile directory: {}", profileDir, e);
            return;
        }

        Path jsonl = profileDir.resolve("bases.jsonl");
        String jsonLine = GSON.toJson(snapshot);
        try {
            Files.writeString(jsonl, jsonLine + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOGGER.warn("Failed to write {}", jsonl, e);
        }

        Path csv = profileDir.resolve("bases.csv");
        try {
            ensureCsvHeader(csv);
            Files.writeString(csv, snapshot.toCsvLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOGGER.warn("Failed to write {}", csv, e);
        }

        Path prettyDir = profileDir.resolve("pretty");
        try {
            Files.createDirectories(prettyDir);
            Path prettyFile = prettyDir.resolve("room_" + snapshot.roomId + ".json");
            Files.writeString(prettyFile, PRETTY_GSON.toJson(snapshot), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            LOGGER.warn("Failed to write pretty snapshot for room {}", snapshot.roomId, e);
        }

        Path summary = profileDir.resolve("bases.txt");
        try {
            Files.writeString(summary, snapshot.toSummaryLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOGGER.warn("Failed to write {}", summary, e);
        }
    }

    private static void ensureCsvHeader(Path csv) throws IOException {
        if (Files.exists(csv)) {
            return;
        }
        Files.writeString(csv, RoomSnapshot.csvHeader() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

    private static Path getProfileDir(String profile) {
        Path desktop = Paths.get(System.getProperty("user.home"), "Desktop");
        return desktop.resolve(DATA_DIR_NAME).resolve(profile);
    }

    private static int getBrymaxPad() {
        try {
            Class<?> cls = Class.forName("com.tuma.brymax.BRYMaxClient");
            Field padField = cls.getDeclaredField("pad");
            padField.setAccessible(true);
            return ((Number) padField.get(null)).intValue();
        } catch (Throwable t) {
            return -1;
        }
    }

    private static String sanitizeProfileName(String raw) {
        if (raw == null) {
            return "";
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (isInvalidPathChar(c)) {
                sb.append('_');
            } else {
                sb.append(c);
            }
        }
        String name = sb.toString().trim();
        if (name.length() > 64) {
            name = name.substring(0, 64).trim();
        }
        return name;
    }

    private static boolean isInvalidPathChar(char c) {
        switch (c) {
            case '<':
            case '>':
            case ':':
            case '"':
            case '/':
            case '\\':
            case '|':
            case '?':
            case '*':
                return true;
            default:
                return false;
        }
    }

    private static void loadConfig() {
        Path cfg = getConfigPath();
        if (!Files.exists(cfg)) {
            config = new LoggerConfig();
            saveConfig();
            return;
        }
        try {
            String json = Files.readString(cfg);
            LoggerConfig parsed = GSON.fromJson(json, LoggerConfig.class);
            config = parsed == null ? new LoggerConfig() : parsed;
        } catch (IOException e) {
            config = new LoggerConfig();
        }
    }

    private static void saveConfig() {
        Path cfg = getConfigPath();
        try {
            Files.createDirectories(cfg.getParent());
            Files.writeString(cfg, GSON.toJson(config), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            LOGGER.warn("Failed to save config {}", cfg, e);
        }
    }

    private static Path getConfigPath() {
        return FabricLoader.getInstance().getConfigDir().resolve("bedrockrooms-base-logger.json");
    }

    private static final class LoggerConfig {
        private String profile = "";
        private double maxBedrockPct = -1.0;
    }

    private static final class RoomWriteState {
        private String lastSignature;
        private int lastLoaded;
        private long lastWriteMs;
    }

    private static final class Bounds {
        private final int minX;
        private final int maxX;
        private final int minY;
        private final int maxY;
        private final int minZ;
        private final int maxZ;

        private Bounds(int minX, int maxX, int minY, int maxY, int minZ, int maxZ) {
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            this.minZ = minZ;
            this.maxZ = maxZ;
        }
    }

    private static final class YMaxSnapshot {
        private final int yMax;
        private final int pad;
        private final int width;
        private final int depth;
        private final int loaded;
        private final int skipped;
        private final int bedrock;
        private final int air;
        private final int other;
        private final double bedrockPct;
        private final double airPct;
        private final double otherPct;

        private YMaxSnapshot(int yMax, int pad, int width, int depth, int loaded, int skipped, int bedrock, int air, int other) {
            this.yMax = yMax;
            this.pad = pad;
            this.width = width;
            this.depth = depth;
            this.loaded = loaded;
            this.skipped = skipped;
            this.bedrock = bedrock;
            this.air = air;
            this.other = other;
            if (loaded <= 0) {
                this.bedrockPct = -1.0;
                this.airPct = -1.0;
                this.otherPct = -1.0;
            } else {
                this.bedrockPct = bedrock * 100.0 / loaded;
                this.airPct = air * 100.0 / loaded;
                this.otherPct = other * 100.0 / loaded;
            }
        }
    }

    private static final class RoomSnapshot {
        private final String timestamp;
        private final String profile;
        private final long roomId;
        private final double centerX;
        private final double centerY;
        private final double centerZ;
        private final int minX;
        private final int maxX;
        private final int minY;
        private final int maxY;
        private final int minZ;
        private final int maxZ;
        private final int roomCells;
        private final int exitCells;
        private final int exitPercent;
        private final int standableCount;
        private final int placeableCount;
        private final boolean sealed;
        private final int exitComponents;
        private final int maxExitComponentSize;
        private final int privateBestScore;
        private final int blastSafeCells;
        private final double blastSafePercent;
        private final int idealPrivateCount;
        private final YMaxSnapshot yMax;

        private RoomSnapshot(
                String profile,
                long roomId,
                double centerX,
                double centerY,
                double centerZ,
                Bounds bounds,
                int roomCells,
                int exitCells,
                int exitPercent,
                int standableCount,
                int placeableCount,
                boolean sealed,
                int exitComponents,
                int maxExitComponentSize,
                int privateBestScore,
                int blastSafeCells,
                double blastSafePercent,
                int idealPrivateCount,
                YMaxSnapshot yMax
        ) {
            this.timestamp = TS_FORMAT.format(Instant.now());
            this.profile = profile;
            this.roomId = roomId;
            this.centerX = centerX;
            this.centerY = centerY;
            this.centerZ = centerZ;
            this.minX = bounds.minX;
            this.maxX = bounds.maxX;
            this.minY = bounds.minY;
            this.maxY = bounds.maxY;
            this.minZ = bounds.minZ;
            this.maxZ = bounds.maxZ;
            this.roomCells = roomCells;
            this.exitCells = exitCells;
            this.exitPercent = exitPercent;
            this.standableCount = standableCount;
            this.placeableCount = placeableCount;
            this.sealed = sealed;
            this.exitComponents = exitComponents;
            this.maxExitComponentSize = maxExitComponentSize;
            this.privateBestScore = privateBestScore;
            this.blastSafeCells = blastSafeCells;
            this.blastSafePercent = blastSafePercent;
            this.idealPrivateCount = idealPrivateCount;
            this.yMax = yMax;
        }

        private static String csvHeader() {
            return String.join(",",
                    "timestamp",
                    "profile",
                    "roomId",
                    "centerX",
                    "centerY",
                    "centerZ",
                    "minX",
                    "maxX",
                    "minY",
                    "maxY",
                    "minZ",
                    "maxZ",
                    "roomCells",
                    "exitCells",
                    "exitPercent",
                    "standableCount",
                    "placeableCount",
                    "sealed",
                    "exitComponents",
                    "maxExitComponentSize",
                    "privateBestScore",
                    "blastSafeCells",
                    "blastSafePercent",
                    "idealPrivateCount",
                    "yMax",
                    "yMaxPad",
                    "yMaxWidth",
                    "yMaxDepth",
                    "yMaxLoaded",
                    "yMaxSkipped",
                    "yMaxBedrock",
                    "yMaxAir",
                    "yMaxOther",
                    "yMaxBedrockPct",
                    "yMaxAirPct",
                    "yMaxOtherPct"
            );
        }

        private String toCsvLine() {
            String[] parts = new String[] {
                    csvEscape(timestamp),
                    csvEscape(profile),
                    String.valueOf(roomId),
                    formatDouble(centerX),
                    formatDouble(centerY),
                    formatDouble(centerZ),
                    String.valueOf(minX),
                    String.valueOf(maxX),
                    String.valueOf(minY),
                    String.valueOf(maxY),
                    String.valueOf(minZ),
                    String.valueOf(maxZ),
                    String.valueOf(roomCells),
                    String.valueOf(exitCells),
                    String.valueOf(exitPercent),
                    String.valueOf(standableCount),
                    String.valueOf(placeableCount),
                    String.valueOf(sealed),
                    String.valueOf(exitComponents),
                    String.valueOf(maxExitComponentSize),
                    String.valueOf(privateBestScore),
                    String.valueOf(blastSafeCells),
                    formatDouble(blastSafePercent),
                    String.valueOf(idealPrivateCount),
                    String.valueOf(yMax.yMax),
                    String.valueOf(yMax.pad),
                    String.valueOf(yMax.width),
                    String.valueOf(yMax.depth),
                    String.valueOf(yMax.loaded),
                    String.valueOf(yMax.skipped),
                    String.valueOf(yMax.bedrock),
                    String.valueOf(yMax.air),
                    String.valueOf(yMax.other),
                    formatDouble(yMax.bedrockPct),
                    formatDouble(yMax.airPct),
                    formatDouble(yMax.otherPct)
            };
            return String.join(",", parts);
        }

        private String signatureKey() {
            return roomCells + ":" + exitCells + ":" + blastSafeCells + ":" + idealPrivateCount + ":" + minX + ":" + maxX + ":" + minZ + ":" + maxZ
                    + ":" + yMax.loaded + ":" + yMax.skipped + ":" + yMax.bedrock + ":" + yMax.air + ":" + yMax.other;
        }

        private String toSummaryLine() {
            String sealedStr = sealed ? "sealed" : "open";
            String blastStr = blastSafeCells >= 0 ? String.valueOf(blastSafeCells) : "-";
            String blastPct = blastSafePercent >= 0 ? formatDouble(blastSafePercent) + "%" : "-";
            String yMaxLoaded = yMax.loaded > 0 ? String.valueOf(yMax.loaded) : "0";
            String yMaxB = yMax.loaded > 0 ? formatDouble(yMax.bedrockPct) + "%" : "-";
            String yMaxA = yMax.loaded > 0 ? formatDouble(yMax.airPct) + "%" : "-";
            String yMaxO = yMax.loaded > 0 ? formatDouble(yMax.otherPct) + "%" : "-";
            return String.format(Locale.ROOT,
                    "%s room=%d center=(%.2f,%.2f,%.2f) size=%d exits=%d(%d%%) %s priv=%d ideal=%d blastSafe=%s(%s) yMax=%d loaded=%s b=%s a=%s o=%s",
                    timestamp, roomId, centerX, centerY, centerZ, roomCells, exitCells, exitPercent,
                    sealedStr, privateBestScore, idealPrivateCount, blastStr, blastPct, yMax.yMax, yMaxLoaded, yMaxB, yMaxA, yMaxO);
        }

        private static String csvEscape(String value) {
            if (value == null) {
                return "";
            }
            String escaped = value.replace("\"", "\"\"");
            return "\"" + escaped + "\"";
        }

        private static String formatDouble(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return "";
            }
            return String.format(Locale.ROOT, "%.3f", value);
        }
    }

    private static final class BrAccess {
        private boolean resolved;
        private boolean available;

        private Method getRoomsMethod;
        private Method valuesMethod;

        private Field roomIdField;
        private Field centerXField;
        private Field centerYField;
        private Field centerZField;
        private Field roomCellsField;
        private Field exitCellsField;
        private Field exitComponentsField;
        private Field maxExitComponentSizeField;
        private Field sealedField;
        private Field standableCountField;
        private Field placeableCountField;
        private Field privateBestScoreField;
        private Field idealPrivateCountField;
        private Field roomCountField;
        private Field blastSafeBlocksField;

        private Method computeBlastSafeMethod;

        private Field cfgInstanceField;
        private Field minBlastSafeCellsToShowField;
        private Field minIdealPrivateCellsToShowField;
        private Field yMaxField;

        private long roomId;
        private double centerX;
        private double centerY;
        private double centerZ;
        private LongCollection roomCells;
        private LongCollection exitCells;
        private int exitComponents;
        private int maxExitComponentSize;
        private boolean sealed;
        private int standableCount;
        private int placeableCount;
        private int privateBestScore;
        private int idealPrivateCount;
        private int roomCount;

        private boolean isAvailable() {
            if (!resolved) {
                resolve();
            }
            return available;
        }

        private void resolve() {
            resolved = true;
            try {
                Class<?> brScanner = Class.forName("com.bedrockrooms.scan.BRScanner");
                getRoomsMethod = brScanner.getMethod("getRooms");

                Class<?> roomClass = Class.forName("com.bedrockrooms.scan.RoomResult");
                roomIdField = roomClass.getField("roomId");
                centerXField = roomClass.getField("centerX");
                centerYField = roomClass.getField("centerY");
                centerZField = roomClass.getField("centerZ");
                roomCellsField = roomClass.getField("roomCells");
                exitCellsField = roomClass.getField("exitCells");
                exitComponentsField = roomClass.getField("exitComponents");
                maxExitComponentSizeField = roomClass.getField("maxExitComponentSize");
                sealedField = roomClass.getField("sealed");
                standableCountField = roomClass.getField("standableCount");
                placeableCountField = roomClass.getField("placeableCount");
                privateBestScoreField = roomClass.getField("privateBestScore");
                idealPrivateCountField = roomClass.getField("idealPrivateCount");
                roomCountField = roomClass.getField("roomCount");
                blastSafeBlocksField = roomClass.getField("blastSafeBlocks");

                computeBlastSafeMethod = brScanner.getDeclaredMethod("computeBlastSafe", ClientWorld.class, roomClass);
                computeBlastSafeMethod.setAccessible(true);

                Class<?> cfgClass = Class.forName("com.bedrockrooms.config.BRConfig");
                cfgInstanceField = cfgClass.getField("INSTANCE");
                minBlastSafeCellsToShowField = cfgClass.getField("minBlastSafeCellsToShow");
                minIdealPrivateCellsToShowField = cfgClass.getField("accurateDamageMinBlastSafeCellsToShow");
                yMaxField = cfgClass.getField("yMax");

                available = true;
            } catch (Throwable t) {
                LOGGER.warn("BedrockRooms not available.", t);
                available = false;
            }
        }

        private Iterable<?> getRoomsIterable() {
            if (!isAvailable()) {
                return null;
            }
            try {
                Object roomsMap = getRoomsMethod.invoke(null);
                if (roomsMap == null) {
                    return null;
                }
                if (valuesMethod == null || valuesMethod.getDeclaringClass() != roomsMap.getClass()) {
                    valuesMethod = roomsMap.getClass().getMethod("values");
                }
                Object valuesObj = valuesMethod.invoke(roomsMap);
                if (valuesObj instanceof Iterable) {
                    return (Iterable<?>) valuesObj;
                }
            } catch (Throwable t) {
                LOGGER.warn("Failed to access BedrockRooms rooms.", t);
            }
            return null;
        }

        private boolean fillRoomFields(Object room) {
            if (!isAvailable() || room == null) {
                return false;
            }
            try {
                roomId = ((Number) roomIdField.get(room)).longValue();
                centerX = ((Number) centerXField.get(room)).doubleValue();
                centerY = ((Number) centerYField.get(room)).doubleValue();
                centerZ = ((Number) centerZField.get(room)).doubleValue();
                Object cellsObj = roomCellsField.get(room);
                if (!(cellsObj instanceof LongCollection)) {
                    return false;
                }
                roomCells = (LongCollection) cellsObj;
                Object exitsObj = exitCellsField.get(room);
                exitCells = exitsObj instanceof LongCollection ? (LongCollection) exitsObj : null;
                exitComponents = ((Number) exitComponentsField.get(room)).intValue();
                maxExitComponentSize = ((Number) maxExitComponentSizeField.get(room)).intValue();
                sealed = (boolean) sealedField.get(room);
                standableCount = ((Number) standableCountField.get(room)).intValue();
                placeableCount = ((Number) placeableCountField.get(room)).intValue();
                privateBestScore = ((Number) privateBestScoreField.get(room)).intValue();
                idealPrivateCount = ((Number) idealPrivateCountField.get(room)).intValue();
                roomCount = ((Number) roomCountField.get(room)).intValue();
                return true;
            } catch (Throwable t) {
                LOGGER.warn("Failed to read BedrockRooms room fields.", t);
                return false;
            }
        }

        private int getMinBlastSafeCellsToShow() {
            if (!isAvailable()) {
                return 0;
            }
            try {
                Object cfg = cfgInstanceField.get(null);
                return ((Number) minBlastSafeCellsToShowField.get(cfg)).intValue();
            } catch (Throwable t) {
                return 0;
            }
        }

        private int getMinIdealPrivateCellsToShow() {
            if (!isAvailable()) {
                return 0;
            }
            try {
                Object cfg = cfgInstanceField.get(null);
                if (minIdealPrivateCellsToShowField == null) {
                    return 0;
                }
                return ((Number) minIdealPrivateCellsToShowField.get(cfg)).intValue();
            } catch (Throwable t) {
                return 0;
            }
        }

        private int getYMax() {
            if (!isAvailable()) {
                return Integer.MIN_VALUE;
            }
            try {
                Object cfg = cfgInstanceField.get(null);
                return ((Number) yMaxField.get(cfg)).intValue();
            } catch (Throwable t) {
                return Integer.MIN_VALUE;
            }
        }

        private int getBlastSafeCount(Object room, ClientWorld world) {
            if (!isAvailable()) {
                return -1;
            }
            try {
                Object safeObj = blastSafeBlocksField.get(room);
                if (safeObj instanceof LongCollection) {
                    return ((LongCollection) safeObj).size();
                }
                if (computeBlastSafeMethod != null) {
                    computeBlastSafeMethod.invoke(null, world, room);
                    Object updated = blastSafeBlocksField.get(room);
                    if (updated instanceof LongCollection) {
                        return ((LongCollection) updated).size();
                    }
                }
            } catch (Throwable t) {
                LOGGER.warn("Failed to compute blast safe cells.", t);
            }
            return -1;
        }
    }
}
