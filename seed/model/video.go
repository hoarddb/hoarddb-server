package model

import "fmt"

func videoID(rank int) string   { return fmt.Sprintf("v%d", rank) }
func channelID(rank int) string { return fmt.Sprintf("c%d", rank) }
func viewerID(rank int) string  { return fmt.Sprintf("w%d", rank) }

// videoSchema is the video layout: a video is a document whose bytes live outside the
// database (Path names them), and the people around it each keep their own stores.
func videoSchema() []BucketDecl {
	return []BucketDecl{
		{Name: "Video", Fields: decl("Owner", "string", "Title", "string", "Path", "string", "Duration", "int", "Views", "int",
			"Comments", "fifoStore", "Likes", "hashStore")},
		{Name: "Channel", Fields: decl("Name", "string", "Uploads", "lifoStore", "Subscribers", "hashStore")},
		{Name: "Viewer", Fields: decl("Name", "string", "Subscriptions", "hashStore", "Progress", "hashStore",
			"History", "fifoStore", "Playlists", "btreeStore")},
	}
}

// buildVideo makes 20 channels, 40 viewers and 100 videos (5 uploads per channel) per scale
// step. Each video has 8 comments and 6 likes; each viewer subscribes to 4 channels (written on
// both sides), has progress on 10 videos, 20 history entries and 2 playlists of 5 items. Views
// is the hot counter of a video, drawn between 0 and 100000, and stays a plain number on the
// document; Path is where the bytes live on external storage, which no engine here holds.
func buildVideo(g *Generator) ([]BucketDecl, []Unit) {
	channels, viewers, videos := 20*g.scale, 40*g.scale, 100*g.scale
	words := newVocabulary(g.seed)
	b := newBuilder()
	at := func(minutes int) int64 { return epoch + int64(minutes)*60 }

	for rank := range channels {
		b.document("Channel", channelID(rank), []Field{{"Name", words.sentence(stream(g.seed, "video-channel", rank), 2)}}, nil)
	}
	for rank := range viewers {
		b.document("Viewer", viewerID(rank), []Field{{"Name", words.sentence(stream(g.seed, "video-viewer", rank), 2)}}, nil)
	}

	// Videos arrive in rank order, five per channel, so a channel's uploads are pushed oldest
	// first and the lifoStore reads them newest first.
	durations := make([]int64, videos)
	uploaded := make([]int64, videos)
	for rank := range videos {
		rng := stream(g.seed, "video-video", rank)
		channel := rank / 5 % channels
		durations[rank] = int64(30 + rng.IntN(7200))
		uploaded[rank] = at(rank * 60)
		path := fmt.Sprintf("library/%d/%02d/v%03d.mp4", 2025+rank/60, 1+rank%12, rank)
		b.document("Video", videoID(rank), []Field{{"Owner", channelID(channel)}, {"Title", words.sentence(rng, 4)}, {"Path", path},
			{"Duration", durations[rank]}, {"Views", int64(rng.IntN(100001))}}, nil)
		b.element("Channel", channelID(channel), "Uploads", "", Field{"video", videoID(rank)}, Field{"at", uploaded[rank]})
		for comment := range 8 {
			b.element("Video", videoID(rank), "Comments", "", Field{"author", viewerID(rng.IntN(viewers))},
				Field{"text", words.sentence(rng, 6)}, Field{"at", uploaded[rank] + int64(comment+1)*60})
		}
		for _, liker := range distinct(rng, viewers, 6, -1) {
			b.element("Video", videoID(rank), "Likes", viewerID(liker), Field{"at", uploaded[rank] + 3600})
		}
		b.setCounters("Video", videoID(rank), Field{"comments_n", b.held("Video", videoID(rank), "Comments")}, Field{"likes_n", b.held("Video", videoID(rank), "Likes")})
	}

	for rank := range viewers {
		rng := stream(g.seed, "video-viewer-stores", rank)
		for _, channel := range distinct(rng, channels, 4, -1) {
			since := at(rank*10 + channel)
			b.element("Channel", channelID(channel), "Subscribers", viewerID(rank), Field{"since", since})
			b.element("Viewer", viewerID(rank), "Subscriptions", channelID(channel), Field{"since", since})
		}
		for _, video := range distinct(rng, videos, 10, -1) {
			b.element("Viewer", viewerID(rank), "Progress", videoID(video),
				Field{"position", int64(rng.IntN(int(durations[video]) + 1))}, Field{"watched_at", uploaded[video] + int64(rng.IntN(86400))})
		}
		for entry := range 20 {
			b.element("Viewer", viewerID(rank), "History", "", Field{"video", videoID(rng.IntN(videos))}, Field{"at", at(videos*60 + rank*100 + entry)})
		}
		for _, playlist := range []string{"watch-later", "favorites"} {
			for position, video := range distinct(rng, videos, 5, -1) {
				b.element("Viewer", viewerID(rank), "Playlists", fmt.Sprintf("%s/%04d", playlist, position),
					Field{"video", videoID(video)}, Field{"added", at(videos*60 + rank*100 + position)})
			}
		}
	}
	for rank := range channels {
		b.setCounters("Channel", channelID(rank), Field{"subscriber_n", b.held("Channel", channelID(rank), "Subscribers")})
	}
	return videoSchema(), b.finish()
}
