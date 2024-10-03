import json
import time


class Timer:
    def __init__(self):
        self.start = time.perf_counter()

    def print(self, msg):
        print(f'{msg}: {time.perf_counter() - self.start} s')


class DDL:
    def __init__(self, file_name):
        with open(file_name) as ddl:
            self.ddl = json.load(ddl)

        self.fields = {}
        for i, field_name in enumerate(self.ddl['fields']):
            self.fields[field_name] = i


class Filter:
    op = None


    def prepare(self, ddl):
        if not self.is_set():
            return
        self.field_no = ddl.fields[self.name]


    def and_cond(self, field_name, op, val):
        self.name = field_name
        self.op = op
        self.val = val
        return self


    def is_set(self):
        return self.op is not None


    def match(self, row):
        if not self.is_set():
            return True
        if self.op == '=':
            return self.val == row[self.field_no]


class Table:
    def __init__(self, file_name, ddl):
        self.read_table(file_name)
        self.ddl = ddl
        self.build_indexes()


    def read_table(self, file_name):
        timer = Timer()
        self.data = {}
        self.curr_key = 1
        for line in open(file_name):
            fields = line[:-1].split(',')
            self.data[self.curr_key] = fields
            self.curr_key = self.curr_key + 1
        timer.print('loading data')


    def build_indexes(self):
        timer = Timer()
        self.indexes = {}
        for idx_name in self.ddl.ddl['indexes']:
            idx = {}
            field_no = self.ddl.fields[idx_name]
            for key, row in self.data.items():
                val = row[field_no]
                if val in idx:
                    idx[val].add(key)
                else:
                    idx[val] = {key}
            self.indexes[idx_name] = idx
        timer.print('building indexes')


    def prepare_filter(self, filter):
        if filter is None:
            return Filter()
        filter.prepare(self.ddl)
        return filter


    def select(self, fields, filter=None):
        timer = Timer()
        filter = self.prepare_filter(filter)
        res = []
        for _, row in self.plan(filter):
            if filter.match(row):
                vals = []
                for field_name in fields:
                    field_no = self.ddl.fields[field_name]
                    vals.append(row[field_no])
                res.append(vals)
        timer.print('select')
        return res


    def delete(self, filter):
        timer = Timer()
        filter = self.prepare_filter(filter)
        to_delete = []
        for key, row in self.plan(filter):
            if filter.match(row):
                to_delete.append(key)
        for key in to_delete:
            for idx_name in self.ddl.ddl['indexes']:
                field_no = self.ddl.fields[idx_name]
                row = self.data[key]
                idx_val = row[field_no]
                self.indexes[idx_name][idx_val].remove(key)
            del self.data[key]
        timer.print('delete')


    def seq_scan(self):
        n = 0
        for key, row in self.data.items():
            n = n + 1
            yield key, row
        print(f'seq_scan: {n} rows')


    def idx_scan(self, idx_name, val):
        n = 0
        for key in self.indexes[idx_name][val]:
            n = n + 1
            yield key, self.data[key]
        print(f'idx_scan on {idx_name}: {n} rows')


    def plan(self, filter):
        if filter.is_set() and filter.name in self.indexes:
            return self.idx_scan(filter.name, filter.val)
        else:
            return self.seq_scan()


def where(field_name, op, val):
    return Filter().and_cond(field_name, op, val)


def read_table(data_file_name, ddl_file_name):
    ddl = DDL(ddl_file_name)
    return Table(data_file_name, ddl)


# phones = read_table('phones.csv', 'phones.ddl')
# print(len(phones.select(['first_name', 'phone'], where('first_name', '=', 'Erin'))))
# phones.delete(where('first_name', '=', 'Erin'))
# print(len(phones.select(['first_name', 'phone'], where('first_name', '=', 'Erin'))))
# print(len(phones.select(['first_name', 'phone'])))
