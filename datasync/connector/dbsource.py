class DBSource:
    def extract_proposals_have_persons(self):
        raise NotImplementedError

    def extract_sessions_have_persons(self, greater_than):
        raise NotImplementedError

    def extract_proposals(self):
        raise NotImplementedError

    def extract_sessions(self):
        raise NotImplementedError
