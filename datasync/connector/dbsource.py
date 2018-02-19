class DBSource:
    def extract_proposals_have_persons(self):
        raise NotImplementedError

    def extract_sessions_have_persons(self, greater_than):
        raise NotImplementedError

    def extract_session_types(self):
        raise NotImplementedError

    def extract_proposals(self):
        raise NotImplementedError

    def extract_sessions(self):
        raise NotImplementedError

    def extract_components(self):
        raise NotImplementedError

    def retrieve_persons_for_session(self, id):
        raise NotImplementedError
