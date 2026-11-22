package model

import "fmt"

func roomID(rank int) string { return fmt.Sprintf("r%d", rank) }

// chatSchema is section 2.2: rooms and users each hold one side of the membership, and the
// Member bucket is the Membership bucket across the kind.
func chatSchema() []BucketDecl {
	return []BucketDecl{
		{Name: "Room", Fields: decl("Name", "string", "Messages", "fifoStore", "Members", "hashStore")},
		{Name: "User", Fields: decl("Name", "string", "Rooms", "hashStore")},
		{Name: "Member", Fields: decl("Room", "string", "User", "string", "Since", "int")},
	}
}

// Chat density. A user page reads 20 rooms (chat.user_rooms, a hash store with no offset) and a
// room page reads 50 messages, with room for an offset of one page: the chat layout seeds every
// user into at least roomsPerUser rooms and every room with messagesPerRoom messages.
const (
	roomsPerUser    = 20
	messagesPerRoom = 100
)

// buildChat makes 40 users and 100 messages per room per scale step, and as many rooms of 3 to 8
// members as it takes to put every user into at least 20 rooms (about 146 rooms per step). Members
// come round a seeded permutation of the users, so every user is in the same number of rooms plus
// or minus one, whatever the seed. A join is written three times, as the layout says:
// Room.Members, User.Rooms and a Member document.
func buildChat(g *Generator) ([]BucketDecl, []Unit) {
	users := 40 * g.scale
	words := newVocabulary(g.seed)
	b := newBuilder()
	for rank := range users {
		b.document("User", userID(rank), []Field{{"Name", words.sentence(stream(g.seed, "chat-user", rank), 2)}}, nil)
	}
	// Room sizes, 3 to 8 each, until the joins give every user roomsPerUser rooms.
	var sizes []int
	for joined := 0; joined < roomsPerUser*users; {
		size := 3 + stream(g.seed, "chat-members", len(sizes)).IntN(6)
		sizes = append(sizes, size)
		joined += size
	}
	rooms := len(sizes)
	for rank := range rooms {
		b.document("Room", roomID(rank), []Field{{"Name", words.sentence(stream(g.seed, "chat-room", rank), 2)}}, nil)
	}
	type join struct {
		room, user int
		since      int64
	}
	var joins []join
	members := make([][]int, rooms)
	order := stream(g.seed, "chat-member-order", 0).Perm(users)
	cursor := 0
	for room, size := range sizes {
		for range size {
			// size is at most 8 and users at least 40, so a window of the cycle never repeats a user.
			user := order[cursor%users]
			cursor++
			since := epoch + int64(room*100+user)*60
			joins = append(joins, join{room, user, since})
			members[room] = append(members[room], user)
			b.element("Room", roomID(room), "Members", userID(user), Field{"user", userID(user)}, Field{"since", since})
			b.element("User", userID(user), "Rooms", roomID(room), Field{"room", roomID(room)}, Field{"since", since})
		}
	}
	for room := range rooms {
		rng := stream(g.seed, "chat-messages", room)
		for message := range messagesPerRoom {
			author := members[room][rng.IntN(len(members[room]))]
			b.element("Room", roomID(room), "Messages", "", Field{"author", userID(author)},
				Field{"text", words.sentence(rng, 3+rng.IntN(12))}, Field{"at", epoch + int64(room*1000+message)*30})
		}
		b.setCounters("Room", roomID(room), Field{"member_n", int64(len(members[room]))}, Field{"message_n", b.held("Room", roomID(room), "Messages")})
	}
	for _, j := range joins {
		b.document("Member", roomID(j.room)+"/"+userID(j.user), []Field{{"Room", roomID(j.room)}, {"User", userID(j.user)}, {"Since", j.since}}, nil)
	}
	return chatSchema(), b.finish()
}
