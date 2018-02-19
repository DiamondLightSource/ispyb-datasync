class DBTarget:
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

    def delete_proposal(self, id):
        raise NotImplementedError

    def update_proposal_code(self, code, id):
        raise NotImplementedError

    def update_proposal(self, title, src_id, id):
        raise NotImplementedError

    def insert_proposal(self, proposal, title, src_id): # proposal = code+number
        raise NotImplementedError

    def delete_session(self, id):
        raise NotImplementedError

    def update_session(self, src_id, beamline, start_date, end_date, local_contacts, scheduled, id):
        raise NotImplementedError

    def insert_session(self, src_id, beamline, comments, start_date, end_date, session_name, beamline_operators, scheduled, persons_rs=None):
        raise NotImplementedError
