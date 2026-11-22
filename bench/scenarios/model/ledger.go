package model

import "fmt"

func accountID(rank int) string { return fmt.Sprintf("a%d", rank) }

// ledgerSchema is section 2.3: one bucket, a btree of expenses and a hash of invoices.
func ledgerSchema() []BucketDecl {
	return []BucketDecl{
		{Name: "Account", Fields: decl("Name", "string", "Currency", "string", "Expenses", "btreeStore", "Invoices", "hashStore")},
	}
}

// AggregateDecl is one maintained aggregate of a store field as `create bucket` declares it
// (the store types page of the user docs): Kind is "sum", "sum by" or "count by", Field the summed element
// path (empty for count by) and By the grouping path (empty for sum).
type AggregateDecl struct{ Kind, Field, By string }

// LedgerAggregates are the aggregates the ledger declares when the leg asks for them
// (-layout-variant common+agg), keyed by "Bucket.Store": the whole-store expense sum, the sum and
// the count per category, and the invoice sum. They are the engine's own answer to the
// expense_total and invoice_total columns the SQL arms keep in every write. The one definition
// for the seeder, the adapter's probe and the audit, so no second declaration exists.
func LedgerAggregates() map[string][]AggregateDecl {
	return map[string][]AggregateDecl{
		"Account.Expenses": {{Kind: "sum", Field: "amount"}, {Kind: "sum by", Field: "amount", By: "category"}, {Kind: "count by", By: "category"}},
		"Account.Invoices": {{Kind: "sum", Field: "amount"}},
	}
}

// Ledger density: a page of 50 expenses and 20 invoices, each with room for a page offset, so
// every account holds twice that.
const (
	expensesPerAccount = 100
	invoicesPerAccount = 40
)

// buildLedger makes 30 accounts per scale step with 100 expenses and 40 invoices each. An
// expense key is "<time>-<id>" so the btree lists in time order; the counters carry the sum a
// page read computes by hand in the document stores.
func buildLedger(g *Generator) ([]BucketDecl, []Unit) {
	accounts := 30 * g.scale
	expenses := expensesPerAccount
	if g.opts.LedgerExpenses > 0 {
		expenses = g.opts.LedgerExpenses
	}
	words := newVocabulary(g.seed)
	categories := []string{"travel", "meals", "software", "office", "hardware", "fees"}
	currencies := []string{"EUR", "USD", "GBP"}
	b := newBuilder()
	for rank := range accounts {
		rng := stream(g.seed, "ledger-account", rank)
		currency := currencies[rng.IntN(len(currencies))]
		b.document("Account", accountID(rank), []Field{{"Name", words.sentence(rng, 2)}, {"Currency", currency}}, nil)
		var expenseTotal, invoiceTotal int64
		for expense := range expenses {
			amount := int64(100 + rng.IntN(50000))
			expenseTotal += amount
			stamp := epoch + int64(expense)*3600 + int64(rng.IntN(3600))
			b.element("Account", accountID(rank), "Expenses", fmt.Sprintf("%010d-%04d", stamp, expense),
				Field{"amount", amount}, Field{"category", categories[rng.IntN(len(categories))]}, Field{"currency", currency})
		}
		for invoice := range invoicesPerAccount {
			amount := int64(1000 + rng.IntN(500000))
			invoiceTotal += amount
			number := fmt.Sprintf("INV-%06d", rank*invoicesPerAccount+invoice)
			b.element("Account", accountID(rank), "Invoices", number,
				Field{"number", number}, Field{"amount", amount}, Field{"customer", words.sentence(rng, 2)}, Field{"due", epoch + int64(invoice+30)*86400})
		}
		b.setCounters("Account", accountID(rank), Field{"expense_n", int64(expenses)}, Field{"expense_total", expenseTotal},
			Field{"invoice_n", int64(invoicesPerAccount)}, Field{"invoice_total", invoiceTotal})
	}
	return ledgerSchema(), b.finish()
}
