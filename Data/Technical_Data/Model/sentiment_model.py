

class sentiment_model:
    def __init__(self):
        self.symbols = []
        self.source = None
        self.crawl_date = None
        self.title = None
        self.url = None
        self.published_date = None
        self.id = None
        self.tags = []
        self.description = None
        self.sentiment_data = {
            'negative' : None,
            'neutral' : None,
            'positive' : None,
            'compound' : None
        }