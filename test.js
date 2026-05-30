const { JsStreamex } = require("./");

const stream = new JsStreamex();

stream.trades(
	"binance",
	"BTCUSDT",

	(trade) => {
		console.log(
			JSON.parse(trade)
		);
	}
);