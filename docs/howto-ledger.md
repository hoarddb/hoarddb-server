# How-to: a ledger

An expense tracker or small ledger keeps accounts. Under each account sit its
expenses, written all day and read back by date, and its invoices, looked up by
number. The number people want is a sum: what this account spent this month. The
account is the entity, and everything the application reads is about one
account at a time.

## The application and its reads

| Read or write | How often | Shape |
| --- | --- | --- |
| Record an expense on an account | constantly | one keyed write, time then id |
| Read an account's expenses for a date range | every statement view | a key range of one account |
| Sum an account's expenses over that range | every statement view | the amounts of that range, added |
| Record an invoice | occasionally | a keyed write by invoice number |
| Look up an invoice by number | on demand | one key on one account |
| List the accounts in one currency | admin screens | the accounts whose currency is this one |

Everything on the hot path is about one account. Only the last read crosses the
kind, and only by one field.

## The layouts this application usually gets

**A relational schema.** Three tables: `accounts`, `expenses (account_id, at, id,
amount, category, currency)` with a key on `(account_id, at, id)` so a date range
is a range scan, and `invoices (account_id, number, ...)`. A sum is
`SELECT SUM(amount)` over that range, which is the natural answer and the reason
most ledgers start here. What goes wrong is the cross-account question: a sum by
category over every account is a scan of the whole expenses table, and the table
grows without limit.

**A document store.** An `expenses` collection indexed on `(account, at)` and an
`invoices` collection indexed on `(account, number)`, with the sum computed by an
aggregation. Embedding the expenses in the account's document is the other
choice, and it fails the way every unbounded array does. The aggregation is
convenient, and it reads every matching document each time.

**An in-memory data structure server.** A sorted set per account scored by time,
with the expense encoded into the member, and a hash per account for invoices. The
structures match the reads. What it lacks is a schema, so the amount and category
are packed into a string the application has to unpack, and a sum is a scan of the
set done in the client.

## The hoardDB layout

```
create bucket Account in ledger { Name string, Currency string, Expenses btreeStore, Invoices hashStore }
```

One bucket. An account carries its own expenses and its own invoices.

**Why `Expenses` is a btree on the account.** A btree keeps its elements in key
order. The key is the time, then the id: `1700003090-0000`, with the time padded
to one width so string order is time order, and the id after it so two expenses in
the same second do not collide. A date range is then a key range of one account's
store, and the account's expenses read back oldest first. Because the store belongs
to the account, no other account's expenses are in the way.

**Why `Invoices` is a hash on the account.** An invoice is written once and looked
up by its number. A hash gives `put` and `get` by key on one account. There is no
read that wants invoices in order, so no order is paid for.

**Why the sum is read and added in the application.** The application lists the
account's expenses and adds the amounts. That is correct for the few thousand
expenses an account holds in a period. A total maintained by the engine, so the
sum is one read instead of a scan, is not available yet.

**Why `Currency` is a plain field.** "The accounts in this currency" is the one
read that crosses the kind. `list` with an equality filter on a declared scalar
serves it, and it is the right tool for a read that runs on an admin screen.

## What changes in the application

- An expense is one `put` whose key starts with its time. There is no expenses
  table to index and no second write.
- A statement is a read of one account's expenses, and the sum is the amounts
  added together by the code that shows it. The application stops at the end of
  its date range.
- An invoice is a `put` and a `get` by number, with a missing number answered by a
  typed not-found error that names the statement to list what exists.
- The application keeps no running total of its own unless it wants one; if it
  does, it must keep it equal to the expenses itself.

## Cost and limits

**What a document costs.** An account with its expenses and invoices is one
document with two stores: a few keys in the node's one shared store, no file, no
descriptor and no goroutine of its own. The cost of recording an expense does not
grow with the number of accounts.

**What this layout does not serve.**

- A sum across every account by category in one read. Expenses live on their
  account, so that is one read per account, added by the application.
- "All expenses over 1000 across all accounts". The same scan of every account;
  there is no index across the kind.
- A sum kept by the engine. The sum is computed on read today; a maintained sum is
  not available yet.
- A bounded read of a date range written as one statement in this walkthrough. The
  page below lists the account's expenses in key order, and the application stops
  at the date it wants.
- An account whose expenses are so many that listing them for a sum is too slow.
  That account needs a total kept by the application.

If one of those is on your hot path, this is not the layout for it, and the
[Limitations](limitations.md) page is the honest next read.

## Try it

Start a server and seed the layout as described on the [How-to](howto.md) page,
with `-layout ledger`. The transcripts below were captured against that data:
thirty accounts, each with forty expenses and ten invoices. The seeded amounts and
names are random, so the shape is real and the figures are not.

An account and its expenses, in key order.

```
[h]oardDB [scenarios_ledger]> use scenarios_ledger
Switched to database 'scenarios_ledger'
[h]oardDB [scenarios_ledger]> db.Account["a17"].get()
{"Currency": "USD", "Expenses": "btreeStore (40 elements)", "Invoices": "hashStore (10 elements)", "Name": "jbkyoqaaw szbackqei", "_id_": "a17"}
[h]oardDB [scenarios_ledger]> db.Account["a17"].Expenses.count()
40
[h]oardDB [scenarios_ledger]> db.Account["a17"].Expenses.list({limit: 4})
1700003090-0000  {"amount": 29059, "category": "travel", "currency": "USD"}
1700003790-0001  {"amount": 16346, "category": "office", "currency": "USD"}
1700009579-0002  {"amount": 10168, "category": "travel", "currency": "USD"}
1700011135-0003  {"amount": 26464, "category": "travel", "currency": "USD"}
more: db.Account["a17"].Expenses.list({limit: 4, cursor: "50313730303031313133352d303030330000"})
```

Record an expense, whose key is the time then the id, and read it back.

```
[h]oardDB [scenarios_ledger]> db.Account["a17"].Expenses.put("1700500000-0040", {amount: 12000, category: "travel", currency: "USD"})
OK  put "1700500000-0040": Expenses now holds 41 elements
[h]oardDB [scenarios_ledger]> db.Account["a17"].Expenses.get("1700500000-0040")
{"amount": 12000, "category": "travel", "currency": "USD"}
[h]oardDB [scenarios_ledger]> db.Account["a17"].Expenses.count()
41
```

The sum is the application's: it lists the account's expenses and adds the
`amount` of each. With a `limit` of 100 the whole account came back in one page
here, 41 elements, and the amounts add up to 1014537. The list is not shown in full.

Invoices, by number, with a deliberate miss.

```
[h]oardDB [scenarios_ledger]> db.Account["a17"].Invoices.list({limit: 3})
INV-000170  {"amount": 434782, "customer": "ehjh px", "due": 1702592000, "number": "INV-000170"}
INV-000171  {"amount": 345341, "customer": "yr inv", "due": 1702678400, "number": "INV-000171"}
INV-000172  {"amount": 311594, "customer": "ison aoqgkp", "due": 1702764800, "number": "INV-000172"}
more: db.Account["a17"].Invoices.list({limit: 3, cursor: "50494e562d3030303137320000"})
[h]oardDB [scenarios_ledger]> db.Account["a17"].Invoices.put("INV-009001", {number: "INV-009001", customer: "acme", amount: 500000, due: 1703000000})
OK  put "INV-009001": Invoices now holds 11 elements
[h]oardDB [scenarios_ledger]> db.Account["a17"].Invoices.get("INV-009001")
{"amount": 500000, "customer": "acme", "due": 1703000000, "number": "INV-009001"}
[h]oardDB [scenarios_ledger]> db.Account["a17"].Invoices.get("INV-000999")
CLI error: NOT_FOUND: Invoices of "a17" in scenarios_ledger.Account has no key "INV-000999". Run 'db.Account["a17"].Invoices.list()' to see what it holds, or 'db.Account["a17"].Invoices.put("INV-000999", {…})' to add one
```

The one read that crosses the kind, by an equality filter on a declared field.

```
[h]oardDB [scenarios_ledger]> db.Account.list({filter: {Currency: "EUR"}, limit: 2})
{"Currency": "EUR", "Expenses": "btreeStore (40 elements)", "Invoices": "hashStore (10 elements)", "Name": "qbuejd epeftsxdc", "_id_": "a3"}
{"Currency": "EUR", "Expenses": "btreeStore (40 elements)", "Invoices": "hashStore (10 elements)", "Name": "iafh ulmco", "_id_": "a5"}
more: db.Account.list({filter: {Currency: "EUR"}, limit: 2, cursor: "010000000000000003d0a62a8ad05b8f600160055061350000d0335258"})
[h]oardDB [scenarios_ledger]> db.Account.count()
30
```
