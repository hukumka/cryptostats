import ccxt

def api_by_name(exchange_name):
    if exchange_name == 'exmo':
        return ccxt.exmo()
    elif exchange_name == 'yobit':
        return ccxt.yobit()
    elif exchange_name == 'hitbtc':
        return ccxt.hitbtc()
    elif exchange_name == 'livecoin':
        return ccxt.livecoin()
    else:
        raise Exception("Unknown exchange")


class PairGraph:
    def __init__(self, exchange):
        self.exchange = exchange
        self.markets = exchange.fetch_markets()
        self.currencies = all_currencies(self.markets)
        self.indexes = {c: i for i, c in enumerate(self.currencies)}

        # determine convert paths
        self.path = [[None for _ in self.indexes] for _ in self.indexes]
        self.route = [[None for _ in self.indexes] for _ in self.indexes]
        for p in self.markets:
            i = self.indexes[p['base']]
            j = self.indexes[p['quote']]
            self.path[i][j] = [i, j]
            self.route[i][j] = True
            self.path[j][i] = [j, i]
            self.route[j][i] = False

        # Floyd-Warshall algorithm
        size = len(self.indexes)

        for k in range(size):
            for i in range(size):
                for j in range(size):
                    self.path[i][j] = self.shorter_path(self.path[i][k], self.path[k][j], self.path[i][j])
        # convert table cash
        self.convert_table = [[None for _ in self.indexes] for _ in self.indexes]

    def convert_currency(self, from_, into, value):
        if from_ == into:
            return value
        path = self.path[self.indexes[from_]][self.indexes[into]]
        for i in range(len(path)-1):
            f = path[i]
            i = path[i+1]
            convert = self.convert_multiplier(f, i)
            value *= convert
        return value

    def convert_multiplier(self, from_, into):
        cashed = self.convert_table[from_][into]
        if cashed is not None:
            return  cashed

        if self.route[from_][into]:
            pair = self.currencies[from_] + "/" + self.currencies[into]
            book = self.exchange.fetch_order_book(pair)
            convert = (book['bids'][0][0] + book['asks'][0][0])/2
        else:
            pair = self.currencies[into] + "/" + self.currencies[from_]
            book = self.exchange.fetch_order_book(pair)
            convert = 2/(book['bids'][0][0] + book['asks'][0][0])

        self.convert_table[from_][into] = convert
        self.convert_table[into][from_] = 1/convert
        return convert
                    
    @staticmethod
    def shorter_path(left_add, right_add, comp):
        if left_add is None or right_add is None:
            return comp
        elif comp is None:
            assert left_add[-1] == right_add[0]
            return left_add + right_add[1:]
        elif len(comp) > len(left_add) + len(right_add):
            assert left_add[-1] == right_add[0]
            return left_add + right_add[1:]
        else:
            return comp
        

def all_currencies(markets):
    res = set()
    for m in markets:
        res.add(m['base'])
        res.add(m['quote'])
    return list(res)

if __name__ == '__main__':
    PairGraph(ccxt.exmo()).convert_currency("WAVES", "HBZ", 1)
