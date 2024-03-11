package alert

import (
	"context"

	"github.com/getnimbus/ultrago/u_logger"
	"github.com/gtuk/discordwebhook"

	"feng-sui-core/internal/conf"
)

func AlertDiscord(ctx context.Context, message string) {
	ctx, logger := u_logger.GetLogger(ctx)

	if conf.Config.DiscordWebhook != "" {
		message := discordwebhook.Message{
			Content: &message,
		}
		if err := discordwebhook.SendMessage(conf.Config.DiscordWebhook, message); err != nil {
			logger.Warnf("failed to send message to discord: %v", err)
		}
	}
}
