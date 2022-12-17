import unittest
from pathlib import Path
from mrfutils import (MRFOpen, MRFObjectBuilder, hashdict)

class TestMRFObjectBuilder(unittest.TestCase):

    def test_p_refs_map(self):

        loc = 'test/test.json.gz'
        npi_set = {1111111111, 5555555555, 2020202020}

        with MRFOpen(loc) as f:
            m = MRFObjectBuilder(f)
            m.ffwd(('', 'map_key', 'provider_references'))
            p_refs_map = m.collect_p_refs(npi_set)

        self.assertTrue(p_refs_map[0][0]['npi'] == [1111111111])
        self.assertTrue(p_refs_map[1][0]['npi'] == [2020202020, 1111111111])

    def test_npis(self):

        loc = 'test/test.json.gz'
        npi_set = {1111111111, 5555555555, 2020202020}
        code_set = {('TS-TST', '0000')}

        with MRFOpen(loc) as f:
            m = MRFObjectBuilder(f)
            root_data = m.collect_root()
            root_data['url'] = Path(loc).stem.split('.')[0]
            root_hash_key = hashdict(root_data)
            root_data['root_hash_key'] = root_hash_key

            p_refs_map = m.collect_p_refs(npi_set)
            m.ffwd(('', 'map_key', 'in_network'))
            g = m.gen_innet_items(npi_set, code_set, p_refs_map)
            item_data = next(g)

        negotiated_rates = item_data['negotiated_rates']

        self.assertTrue(negotiated_rates[0]['provider_groups'][0]['npi'] == [5555555555])
        self.assertTrue(negotiated_rates[0]['provider_groups'][1]['npi'] == [1111111111])
        self.assertTrue(negotiated_rates[0]['provider_groups'][2]['npi'] == [2020202020, 1111111111])


if __name__ == '__main__':
    unittest.main()

    