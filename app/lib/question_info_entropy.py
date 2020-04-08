from app import general_pool_criteria,general_pool_aact


def filter_nct_ids_by_pre_questions(answer_list):
    age = answer_list[0]
    gender = answer_list[1]
    domain = answer_list[2]
    user_picked_time = answer_list[3]
    stat = answer_list[4]
    preg = answer_list[5]
    # query database filter nctids
    return []



def find_annotated_nct_id_list():
    annotated_working_list =[]
    conn = general_pool_criteria.connection()
    cur = conn.cursor()
    sql ='''
        select distinct(nct_id)
        from dbo.aact_trial_info_us_only;
    '''
    cur.execute(sql)
    nctids = cur.fetchall()
    conn.close()
    cur.close()

    if len(nctids) > 0:
        for nctid in nctids:
            annotated_working_list.append(str(nctid[0]))
    return annotated_working_list



def init_working_nct_id_list(rnct):
    '''
    initialize working nct id list
    :param rnct: returned from ctgov search results [...,'NCT02733523;3431','NCT02075840;3432',...]
    :return: a working nct id list
    '''
    working_nct_id_list = []
    annotated_working_list = find_annotated_nct_id_list()
    print annotated_working_list
    for record in rnct:
        if record.split(';')[0] in annotated_working_list:
            working_nct_id_list.append([record.split(';')[0], int(record.split(';')[1]), 0])
    return working_nct_id_list


def init_working_nct_id_list_original(rnct):
    '''
    initialize working nct id list
    :param rnct: returned from ctgov search results [...,'NCT02733523;3431','NCT02075840;3432',...]
    :return: a working nct id list
    '''
    working_nct_id_list  = [[record.split(';')[0], int(record.split(';')[1]), 0] for record in rnct]
    return working_nct_id_list

# working_nct_id_list = [['NCT02901717', 3431, 0], ['NCT01287182', 3432, 0],['NCT01035944', 3432, 0],['NCT00562068', 3431, 1], ['NCT00742300', 3431, 2]]
# question_answer_list = [{'answer': {}, 'question': {'domain': 'condition', 'entity_text': 'pregnant'}} ]
def find_new_question(question_answer_list,working_nct_id_list, domain='all'):
    '''
    find new question by frequency.
    alternatively, information entropy should be considered sum(plog(p))
    :param question_answer_list: questions already answered or skipped with their corresponding answers
    :param working_nct_id_list: a working nct id list
    :return: a updated question_answer_list by appending a new question

    Example
    working_nct_id_list = [['NCT02901717', 3431, 0], ['NCT01287182', 3432, 0],['NCT01035944', 3432, 0],['NCT00562068', 3431, 1], ['NCT00742300', 3431, 2]]
    question_answer_list = [{'answer': {}, 'question': (3, u'pregnant')}]
    '''
    # working_nct_id_frame = pd.DataFrame(working_nct_id_list,columns=['nct_id', 'ctgov_rank', 'num_of_question'])
    working_nct_id_0 = [record[0] for record in working_nct_id_list if record[2] == 0]


    working_nct_id_0_len = len(working_nct_id_0)
    placeholders1 = ",".join("?" * working_nct_id_0_len)
    ########################################################################################################################
    # placeholders1 = ",".join("?" * 2000)
    # working_nct_id_0 = [record[0] for record in working_nct_id_list if record[2] == 0][0:2000]
    # ERROR is raised if the nct list is larger than 2000
    # Use subsampling to solve this issue.
    # Select the first 2000 is better.
    if len(working_nct_id_0) > 2000:
        placeholders1 = ",".join("?" * 2000)
        working_nct_id_0 = working_nct_id_0[0:2000]
    ########################################################################################################################
    domain = domain.lower()
    conn = general_pool_criteria.connection()
    cur = conn.cursor()
    if domain !='all':
        table_name = 'dbo.all_criteria'
        placeholders2 = '?'
        active_question_0 = [qa['question']['entity_text'] for qa in question_answer_list if qa['question']['domain'] == domain]
        placeholders3 = ",".join("?" * len(active_question_0))
        params = []
        params.extend(working_nct_id_0)
        params.extend([domain])

        if len(active_question_0) == 0:

            sql = '''
                    SELECT TOP(1) sum(PlogP) AS IE, concept_name
                    FROM(
                        select concept_name, include, count, -(count/%s)*LOG((count/%s)) AS PlogP
                        FROM
                            (
                        select CAST(count(distinct nct_id) AS [float]) AS count, concept_name, include
                            from %s
                            where nct_id in (%s) and concept_name is NOT NULL
                            and domain = %s
                            group by concept_name, include 
                        ) X
                    ) X
                    GROUP BY concept_name
                    ORDER BY sum(X.PlogP) DESC
                ''' % (working_nct_id_0_len,working_nct_id_0_len,table_name, placeholders1,placeholders2)
        else:
            params.extend(active_question_0)
            sql = '''
                    SELECT TOP(1) sum(PlogP) AS IE, concept_name
                    FROM(
                        select concept_name, include, count, -(count/%s)*LOG((count/%s)) AS PlogP
                        FROM
                            (
                        select CAST(count(distinct nct_id) AS [float]) AS count, concept_name, include
                            from %s
                            where nct_id in (%s) and concept_name is NOT NULL
                            and domain = %s
                            and concept_name not in (%s)
                            group by concept_name, include 
                        ) X
                    ) X
                    GROUP BY concept_name
                    ORDER BY sum(X.PlogP) DESC
                ''' % (working_nct_id_0_len,working_nct_id_0_len,table_name, placeholders1, placeholders2, placeholders3)


        cur.execute(sql, params)
        next_concept = cur.fetchall()
        conn.close()
        cur.close()

        if len(next_concept) > 0:
            this_q = {'question': {'domain': domain, 'entity_text': next_concept[0][1]}}
        else:
            this_q = {'question': {'domain': domain, 'entity_text': 'NQF'}}
        question_answer_list.append(this_q)

    else:
        active_question_0 = [qa['question']['entity_text'] for qa in question_answer_list]
        placeholders2 = ",".join("?" * len(active_question_0))
        params = []
        params.extend(working_nct_id_0)

        if len(active_question_0) == 0:
            sql = '''
                    SELECT TOP(1) sum(PlogP) AS IE, concept_name
                    FROM(
                        select concept_name, include, count, -(count/%s)*LOG((count/%s)) AS PlogP
                        FROM
                            (
                        select CAST(count(distinct nct_id) AS [float]) AS count, concept_name, include
                            from dbo.all_criteria
                            where nct_id in (%s) and concept_name is NOT NULL
                            group by concept_name, include 
                        ) X
                    ) X
                    GROUP BY concept_name
                    ORDER BY sum(X.PlogP) DESC
                ''' % (working_nct_id_0_len,working_nct_id_0_len, placeholders1)
        else:
            params.extend(active_question_0)
            sql = '''
                    SELECT TOP(1) sum(PlogP) AS IE, concept_name
                    FROM(
                        select concept_name, include, count, -(count/%s)*LOG((count/%s)) AS PlogP
                        FROM
                            (
                        select CAST(count(distinct nct_id) AS [float]) AS count, concept_name, include
                            from dbo.all_criteria
                            where nct_id in (%s) and concept_name is NOT NULL
                            and concept_name not in (%s)
                            group by concept_name, include 
                        ) X
                    ) X
                    GROUP BY concept_name
                    ORDER BY sum(X.PlogP) DESC
                ''' % (working_nct_id_0_len,working_nct_id_0_len, placeholders1,placeholders2)
        cur.execute(sql, params)
        next_concept = cur.fetchall()
        conn.close()
        cur.close()

        if len(next_concept) > 0:
            this_q = {'question': {'domain': 'condition', 'entity_text': next_concept[0][1]}}
        else:
            this_q = {'question': {'domain': 'condition', 'entity_text': 'NQF'}}
        question_answer_list.append(this_q)

    return question_answer_list


def find_nct_details(working_nct_id_list,npag):
    '''
    find nct details by connecting to AACT
    :param working_nct_id_list:
    :param npag: page number of result to return
    :return: nct_details_for_this_page is a list of list [[nct_id1, nct_title, nct_summary],[]]. similar to (n, nct) = ctgov.search (stxt, npag)
    '''
    working_nct_id_0 = [(record[0], record[1]) for record in working_nct_id_list if record[2] == 0]
    srt_working_nct_id_0 = sorted(working_nct_id_0, key=lambda x: x[1], reverse=False)
    start_idx = (npag - 1) * 20
    end_idx = min(len(srt_working_nct_id_0), npag * 20)
    nct_id_this_page = [srt[0] for srt in srt_working_nct_id_0[start_idx:end_idx]]
    nct_id_rank = {}
    for srt in srt_working_nct_id_0[start_idx:end_idx]:
        nct_id_rank[srt[0]] = srt[1]

    # nct_id_this_page = ['NCT02901717','NCT01287182']
    sql = '''
            select c.nct_id,s.brief_title,c.name
            from studies AS s
            left join conditions AS c
            on s.nct_id = c.nct_id
            where s.nct_id in %s
        '''
    conn = general_pool_aact.connection()
    cur = conn.cursor()
    cur.execute(sql, (tuple(nct_id_this_page),))
    details = cur.fetchall()
    conn.close()
    cur.close()
    # join condition
    nct_id_condition = {}
    nct_id_title = {}
    for r in details:
        if r[0] not in nct_id_condition.keys():
            nct_id_condition[r[0]] = r[2]
        else:
            nct_id_condition[r[0]] += ',' + r[2]

        if r[0] not in nct_id_title.keys():
            nct_id_title[r[0]] = r[1]

    nct_details_for_this_page = [[nct_id,nct_id_rank[nct_id],nct_id_title[nct_id],nct_id_condition[nct_id]] for nct_id in nct_id_condition.keys()]
    nct_details_for_this_page = sorted(nct_details_for_this_page, key=lambda x: x[1], reverse=False)
    return nct_details_for_this_page

def find_size_of_active_trials(working_nct_id_list):
    '''
    find size of the remaining trials
    :param working_nct_id_list:
    :return: size
    '''
    size = 0
    working_nct_id_0 = [record[0] for record in working_nct_id_list if record[2] == 0]
    size = len(working_nct_id_0)
    return size

# working_nct_id_list = [['NCT02901717', 3431, 0], ['NCT01287182', 3432, 0],['NCT01035944', 3432, 0],['NCT00562068', 3431, 1], ['NCT00742300', 3431, 2]]
# question_answer_list = [{'answer':{'include':'EXC'},'question': {'domain': 'condition', 'entity_text': 'pregnant'}} ]
def update_working_nct_id_list(question_answer_list,working_nct_id_list):
    '''
    update working_nct_id_list by comparing question_answer_list with criteria knowledge base
    :param question_answer_list:
    :param working_nct_id_list:
    :return: an updated working_nct_id_list

    working_nct_id_list = [('NCT02901717', 3431, 0), ('NCT01287182', 3432, 0),('NCT01035944', 3432, 0),('NCT00562068', 3431, 1),('NCT00742300', 3431, 2),]
    question_answer_list = [{'answer': {}, 'question': (3, u'pregnant')}]
    '''
    question_number = len(question_answer_list)

    if question_number > 0:
        this_qa = question_answer_list[question_number-1]
        this_entity_text = this_qa['question']['entity_text']
        this_domain = this_qa['question']['domain']
        table_name = 'dbo.all_criteria'

        if 'answer' not in this_qa.keys():
            return working_nct_id_list

        this_answer = this_qa['answer']
        this_include = this_answer['include']

        if this_domain.lower() != 'measurement':
            rangestart = 0
            rangeend = 0
            if 'rangestart' in this_answer.keys():
                rangestart = this_answer['rangestart']

            if 'rangeend' in this_answer.keys():
                rangeend = this_answer['rangeend']
            
            if this_include == 'INC':
                sql =   '''
                        select distinct nct_id from %s
                        where concept_name in ('%s')
                        and 
                        (
                            (include =0 and before_days >= %s)
                            or 
                            (include =1 and before_days < %s)
                        )
                        ''' % (table_name,this_entity_text,rangeend,rangeend)
            else:
                sql =   '''
                        select distinct nct_id from %s
                        where concept_name in ('%s')
                        and 
                        (
                            (include = 1)
                        )
                        ''' % (table_name,this_entity_text)
        else:
            if 'measurement_value' in this_answer.keys() and this_include == 'INC':
                measurement_value = this_answer['measurement_value']                      
                sql =   '''
                        select distinct nct_id from %s
                        where concept_name in ('%s')
                        and 
                        (
                            (
                                include = 0 and (min <= %s and max >= %s)
                            ) or 
                            (
                                include = 1 and (min > %s or max < %s)
                            )
                        )
                        ''' % (table_name,this_entity_text,measurement_value,measurement_value,measurement_value,measurement_value)
            else:
                sql = '''
                    select top(0) nct_id from %s
                '''% (table_name)

        print(sql)
        conn = general_pool_criteria.connection()
        cur = conn.cursor()
        cur.execute(sql)
        details = cur.fetchall()
        filtered_nct_id = [nct_id[0] for nct_id in details]
        conn.close()
        cur.close()
        for nct_record in working_nct_id_list:
            if nct_record[0] in filtered_nct_id and nct_record[2] == 0:
                nct_record[2] = question_number
        return working_nct_id_list
    else:
        return working_nct_id_list
# print(update_working_nct_id_list(question_answer_list,working_nct_id_list))