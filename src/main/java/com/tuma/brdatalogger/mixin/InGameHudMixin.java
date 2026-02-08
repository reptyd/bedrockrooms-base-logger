package com.tuma.brdatalogger.mixin;

import com.tuma.brdatalogger.BRDataLoggerClient;
import net.minecraft.client.MinecraftClient;
import net.minecraft.client.gui.DrawContext;
import net.minecraft.client.gui.hud.InGameHud;
import net.minecraft.scoreboard.Scoreboard;
import net.minecraft.scoreboard.ScoreboardObjective;
import net.minecraft.scoreboard.ScoreboardPlayerScore;
import net.minecraft.scoreboard.Team;
import net.minecraft.text.Text;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Mixin(InGameHud.class)
public class InGameHudMixin {
    @Inject(method = "renderScoreboardSidebar", at = @At("HEAD"))
    private void brdatalogger_capture(DrawContext context, ScoreboardObjective objective, CallbackInfo ci) {
        if (objective == null) {
            return;
        }
        MinecraftClient client = MinecraftClient.getInstance();
        Scoreboard scoreboard = BRDataLoggerClient.getClientScoreboardForHud(client);
        if (scoreboard == null) {
            return;
        }
        Collection<ScoreboardPlayerScore> scores = scoreboard.getAllPlayerScores(objective);
        if (scores == null || scores.isEmpty()) {
            return;
        }
        List<Text> lines = new ArrayList<>();
        for (ScoreboardPlayerScore score : scores) {
            String name = score.getPlayerName();
            Team team = scoreboard.getPlayerTeam(name);
            Text text = team == null ? Text.literal(name) : Team.decorateName(team, Text.literal(name));
            lines.add(text);
        }
        BRDataLoggerClient.updateHudSnapshot(objective.getDisplayName(), lines);
    }
}
