package shared

type Order struct {
	ID        string `json:"id"`
	ProductID string `json:"product_id"`
}

type Other struct {
	BID     string `json:"bid"`
	Name    string `json:"name"`
	Articul string `json:"articul"`
}
