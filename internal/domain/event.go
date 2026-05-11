package domain

type LoginEvent struct {
	UserID string `json:"user_id"`
	Event  string `json:"event"`
	Price  int    `json:"price"`
}
