import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from datapack.association import BitemporalDataset, DomainError, load_dataset
from datapack.storage import canonical

ROOT=Path(__file__).resolve().parents[1]


def dim(row_id,entity,value_key,value,valid_from,valid_to,known_from,known_to):
    return {
        'row_id':row_id,
        'entity_id':entity,
        value_key:value,
        'valid_from':valid_from,
        'valid_to':valid_to,
        'known_from':known_from,
        'known_to':known_to,
    }


def fact(fact_id,vehicle,event_at,partition='p'):
    return {'fact_id':fact_id,'vehicle':vehicle,'event_at':event_at,'partition':partition}


def by_id(items):
    return {item['fact_id']:item for item in items}


def base_dimensions():
    vehicle=[
        dim('h-old','V','operator','O1','2026-01-01T00:00:00Z',None,
            '2026-01-01T00:00:00Z','2026-11-08T00:00:00Z'),
        dim('h-mid','V','operator','O1','2026-01-01T00:00:00Z','2026-11-01T00:00:00Z',
            '2026-11-08T00:00:00Z',None),
        dim('h-new','V','operator','O2','2026-11-01T00:00:00Z',None,
            '2026-11-08T00:00:00Z',None),
    ]
    operator=[
        dim('o-1','O1','region','east','2026-01-01T00:00:00Z',None,
            '2026-01-01T00:00:00Z',None),
        dim('o-2','O2','region','west','2026-01-01T00:00:00Z',None,
            '2026-01-01T00:00:00Z',None),
    ]
    software=[
        dim('sw-1','V','software','release-a','2026-01-01T00:00:00Z',None,
            '2026-01-01T00:00:00Z',None),
    ]
    return vehicle,operator,software


class SampleAssociationTests(unittest.TestCase):
    def setUp(self):
        self.dataset=load_dataset(ROOT/'samples')

    def test_baseline_explicit_cutoff(self):
        result=self.dataset.join('2026-11-10T12:00:00Z')
        self.assertEqual(result['knowledge_cutoff'],
                         {'mode':'explicit','cutoff':'2026-11-10T12:00:00Z'})
        facts=by_id(result['facts'])
        self.assertEqual(facts['f1']['attribution'],
                         {'operator':'O1','region':'east','software':'release-a'})
        self.assertEqual(facts['f1']['references'],{'vehicle':'h2','operator':'o1','software':'sw1'})
        self.assertEqual(facts['f2']['attribution'],
                         {'operator':'O2','region':'west','software':'release-a'})
        self.assertEqual(facts['f2']['references'],{'vehicle':'h3','operator':'o2','software':'sw1'})
        self.assertNotIn('f3',facts)
        self.assertNotIn('f4',facts)

        errors={}
        for error in result['errors']:
            errors.setdefault(error['fact_id'],[]).append(error)
        self.assertEqual([(e['kind'],e['stage']) for e in errors['f3']],
                         [('gap','vehicle'),('gap','software')])
        self.assertEqual(len(errors['f4']),1)
        self.assertEqual((errors['f4'][0]['kind'],errors['f4'][0]['stage']),
                         ('conflict','software'))
        self.assertEqual(errors['f4'][0]['candidates'],['sw1','sw2'])
        # 时区偏移已归一化，原始事实内容保留。
        self.assertEqual(errors['f4'][0]['event_at'],'2026-11-05T10:00:00+08:00')
        self.assertEqual(errors['f4'][0]['event_at_utc'],'2026-11-05T02:00:00Z')

    def test_historical_cutoff_reproduces_pre_correction_stats(self):
        result=self.dataset.join('2026-11-07T12:00:00Z')
        facts=by_id(result['facts'])
        self.assertEqual(set(facts),{'f1','f2','f4'})
        for fid in ('f1','f2','f4'):
            self.assertEqual(facts[fid]['attribution'],
                             {'operator':'O1','region':'east','software':'release-a'})
            self.assertEqual(facts[fid]['references'],
                             {'vehicle':'h1','operator':'o1','software':'sw1'})
        errors={}
        for error in result['errors']:
            errors.setdefault(error['fact_id'],[]).append(error)
        self.assertEqual([(e['kind'],e['stage']) for e in errors['f3']],
                         [('gap','vehicle'),('gap','software')])

    def test_latest_resolves_once_to_fixed_cutoff(self):
        result=self.dataset.join('latest')
        self.assertEqual(result['knowledge_cutoff']['mode'],'latest')
        self.assertEqual(result['knowledge_cutoff']['cutoff'],'2026-11-09T00:00:00Z')
        observed={row['knowledge_cutoff_utc'] for row in result['facts']}
        observed|={row['knowledge_cutoff_utc'] for row in result['errors']}
        self.assertEqual(observed,{'2026-11-09T00:00:00Z'})

    def test_impact_is_fact_precise_not_vehicle_month_precise(self):
        report=self.dataset.impact('2026-11-07T12:00:00Z','2026-11-10T12:00:00Z')
        changed=by_id(report['changed'])
        self.assertEqual(set(changed),{'f2','f4'})
        self.assertEqual(report['partitions'],['nov'])
        self.assertEqual(report['changed_by_partition'],{'nov':['f2','f4']})

        self.assertEqual(changed['f2']['before']['attribution'],
                         {'operator':'O1','region':'east','software':'release-a'})
        self.assertEqual(changed['f2']['after']['attribution'],
                         {'operator':'O2','region':'west','software':'release-a'})
        self.assertEqual(changed['f2']['changed_fields'],['operator','region'])

        self.assertEqual(changed['f4']['before']['status'],'ok')
        self.assertEqual(changed['f4']['after']['status'],'error')
        self.assertEqual(changed['f4']['changed_fields'],['status'])

        # f1 的引用行从 h1 换成 h2，但归属没有变化：不属于统计影响范围。
        self.assertEqual([item['fact_id'] for item in report['reference_only_changes']],['f1'])
        self.assertNotIn('f1',changed)
        self.assertNotIn('oct',report['partitions'])
        self.assertEqual(report['unchanged_fact_ids'],['f3'])


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        vehicle=[
            dim('b-old','V','operator','O1','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z','2026-11-08T00:00:00Z'),
            dim('b-mid','V','operator','O1','2026-01-01T00:00:00Z','2026-11-01T00:00:00Z',
                '2026-11-08T00:00:00Z',None),
            dim('b-new','V','operator','O2','2026-11-01T00:00:00Z',None,
                '2026-11-08T00:00:00Z',None),
        ]
        operator=[
            dim('o-1','O1','region','east','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z',None),
            dim('o-2','O2','region','west','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z',None),
        ]
        software=[
            dim('sw-1','V','software','release-a','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z',None),
        ]
        facts=[
            fact('before','V','2026-10-31T23:59:59Z'),
            fact('at','V','2026-11-01T00:00:00Z'),
            fact('tz-before','V','2026-11-01T07:59:59+08:00'),
            fact('tz-at','V','2026-11-01T08:00:00+08:00'),
        ]
        self.dataset=BitemporalDataset(vehicle,operator,software,facts)

    def test_valid_interval_is_left_closed_right_open(self):
        facts=by_id(self.dataset.join('2026-11-10T12:00:00Z')['facts'])
        self.assertEqual(facts['before']['attribution']['operator'],'O1')
        self.assertEqual(facts['before']['references']['vehicle'],'b-mid')
        self.assertEqual(facts['at']['attribution']['operator'],'O2')
        self.assertEqual(facts['at']['references']['vehicle'],'b-new')

    def test_timezone_offsets_are_normalized(self):
        facts=by_id(self.dataset.join('2026-11-10T12:00:00Z')['facts'])
        self.assertEqual(facts['tz-before']['event_at_utc'],'2026-10-31T23:59:59Z')
        self.assertEqual(facts['tz-before']['attribution']['operator'],'O1')
        self.assertEqual(facts['tz-at']['event_at_utc'],'2026-11-01T00:00:00Z')
        self.assertEqual(facts['tz-at']['attribution']['operator'],'O2')

    def test_known_interval_is_left_closed_right_open(self):
        just_before=self.dataset.join('2026-11-07T23:59:59Z')
        at_boundary=self.dataset.join('2026-11-08T00:00:00Z')
        before_facts=by_id(just_before['facts'])
        at_facts=by_id(at_boundary['facts'])
        self.assertEqual(before_facts['before']['references']['vehicle'],'b-old')
        self.assertEqual(at_facts['before']['references']['vehicle'],'b-mid')
        self.assertNotIn('b-old',{ref['row_id'] for row in at_facts.values()
                                  for ref in row['chain']})

    def test_ambiguous_local_and_date_only_values_are_rejected(self):
        dataset=BitemporalDataset(*base_dimensions(),facts=[
            fact('naive','V','2026-11-01T00:00:00'),
            fact('date-only','V','2026-11-01'),
            {'fact_id':'no-vehicle','event_at':'2026-11-01T00:00:00Z'},
        ])
        result=dataset.join('2026-11-10T12:00:00Z')
        self.assertEqual(result['facts'],[])
        self.assertEqual(result['invalid_fact_ids'],['date-only','naive','no-vehicle'])
        self.assertTrue(all(error['kind']=='fact_invalid' for error in result['errors']))
        with self.assertRaises(DomainError):
            BitemporalDataset(
                [dim('bad','V','operator','O1','2026-01-01T00:00:00',None,
                     '2026-01-01T00:00:00Z',None)],[],[],[])
        with self.assertRaises(DomainError):
            BitemporalDataset(
                [dim('bad','V','operator','O1','2026-01-01',None,
                     '2026-01-01T00:00:00Z',None)],[],[],[])


class GapConflictAndFactTests(unittest.TestCase):
    def test_gap_and_same_version_overlap_are_side_channel_not_multiplied_facts(self):
        vehicle=[
            dim('v-a','V','operator','O1','2026-01-01T00:00:00Z','2026-03-01T00:00:00Z',
                '2026-01-01T00:00:00Z',None),
            dim('v-b','V','operator','O1','2026-02-01T00:00:00Z','2026-04-01T00:00:00Z',
                '2026-01-01T00:00:00Z',None),
            dim('v-gap','W','operator','O3','2026-01-01T00:00:00Z','2026-02-01T00:00:00Z',
                '2026-01-01T00:00:00Z',None),
        ]
        operator=[
            dim('o-1','O1','region','east','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z',None),
            dim('o-3','O3','region','south','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z',None),
        ]
        software=[
            dim('sw-v','V','software','release-a','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z',None),
            dim('sw-w','W','software','release-b','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z',None),
        ]
        facts=[fact('overlap','V','2026-02-15T00:00:00Z'),
               fact('gap','W','2026-03-01T00:00:00Z')]
        result=BitemporalDataset(vehicle,operator,software,facts).join('2026-11-10T12:00:00Z')
        self.assertEqual(result['facts'],[])
        self.assertEqual(result['counts']['joined'],0)
        overlap_errors=[e for e in result['errors'] if e['fact_id']=='overlap']
        self.assertEqual(len(overlap_errors),1)
        self.assertEqual((overlap_errors[0]['kind'],overlap_errors[0]['stage']),
                         ('conflict','vehicle'))
        self.assertEqual(overlap_errors[0]['candidates'],['v-a','v-b'])
        gap_errors=[e for e in result['errors'] if e['fact_id']=='gap']
        self.assertEqual([(e['kind'],e['stage']) for e in gap_errors],[('gap','vehicle')])
        # 成功事实数不超过事实数，冲突不会放大为多条成功行。
        self.assertLessEqual(result['counts']['joined'],len(facts))

    def test_duplicate_and_conflicting_fact_keys(self):
        vehicle,operator,software=base_dimensions()
        duplicate=fact('dup','V','2026-11-01T00:00:00Z')
        facts=[
            duplicate,
            dict(duplicate),
            fact('bad','V','2026-11-02T00:00:00Z'),
            fact('bad','V','2026-11-03T00:00:00Z'),
            dict(fact('naive','V','2026-11-01T00:00:00'),ingested_at='2027-01-01T00:00:00Z'),
            fact('date-only','V','2026-11-01'),
            {'fact_id':'no-vehicle','event_at':'2026-11-01T00:00:00Z'},
            {'fact_id':'ingest-not-used','vehicle':'V',
             'event_at':'2026-10-31T23:59:59Z','partition':'p',
             'ingested_at':'2030-01-01T00:00:00Z'},
        ]
        result=BitemporalDataset(vehicle,operator,software,facts).join('2026-11-10T12:00:00Z')
        self.assertEqual(result['duplicate_fact_ids'],['dup'])
        self.assertEqual(result['counts']['duplicate_fact_rows'],1)
        self.assertEqual(result['conflict_fact_ids'],['bad'])
        self.assertEqual(result['invalid_fact_ids'],
                         ['date-only','naive','no-vehicle'])
        self.assertEqual([row['fact_id'] for row in result['facts']],['dup','ingest-not-used'])
        ingest=by_id(result['facts'])['ingest-not-used']
        self.assertEqual(ingest['event_at_utc'],'2026-10-31T23:59:59Z')
        self.assertEqual(ingest['attribution']['operator'],'O1')
        self.assertEqual(ingest['attribution']['region'],'east')
        conflict=[e for e in result['errors'] if e['fact_id']=='bad'][0]
        self.assertEqual(conflict['kind'],'fact_conflict')
        self.assertEqual(len(conflict['content_digests']),2)
        naive=[e for e in result['errors'] if e['fact_id']=='naive'][0]
        self.assertEqual(naive['kind'],'fact_invalid')
        self.assertIn('偏移',naive['detail'])
        self.assertNotIn('naive',{row['fact_id'] for row in result['facts']})

    def test_dimension_key_conflict_never_silently_overwrites(self):
        vehicle=[
            dim('same','V','operator','O1','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z',None),
            dim('same','V','operator','O2','2026-01-01T00:00:00Z',None,
                '2026-01-01T00:00:00Z',None),
        ]
        with self.assertRaises(DomainError):
            BitemporalDataset(vehicle,[],[],[])


class ReproducibilityAndCardinalityTests(unittest.TestCase):
    @staticmethod
    def _sample_rows():
        paths={
            'vehicle':'vehicle_history.json',
            'operator':'operator_history.json',
            'software':'software_history.json',
            'facts':'facts.json',
        }
        return {role:json.loads((ROOT/'samples'/name).read_text(encoding='utf-8'))
                for role,name in paths.items()}

    def test_historical_cutoff_output_survives_new_knowledge(self):
        rows=self._sample_rows()
        original=BitemporalDataset(rows['vehicle'],rows['operator'],
                                   rows['software'],rows['facts'])
        vehicle=rows['vehicle']+[dim('h-later','V1','operator','O9',
                                     '2026-01-01T00:00:00Z',None,
                                     '2026-12-01T00:00:00Z',None)]
        updated=BitemporalDataset(vehicle,rows['operator'],rows['software'],rows['facts'])

        old=original.join('2026-11-07T12:00:00Z')
        new=updated.join('2026-11-07T12:00:00Z')
        self.assertEqual(canonical(old['facts']),canonical(new['facts']))
        self.assertEqual(canonical(old['errors']),canonical(new['errors']))
        self.assertEqual(old['knowledge_cutoff'],new['knowledge_cutoff'])

        # latest 会随新知识移动，但历史显式 cutoff 不移动。
        self.assertEqual(original.resolve_cutoff('latest')['cutoff'],'2026-11-09T00:00:00Z')
        self.assertEqual(updated.resolve_cutoff('latest')['cutoff'],'2026-12-01T00:00:00Z')

    def test_cardinality_is_bounded_under_overlaps(self):
        vehicle,operator,software=base_dimensions()
        facts=[fact('f'+str(index),'V','2026-11-02T00:00:00Z','nov')
               for index in range(50)]
        result=BitemporalDataset(vehicle,operator,software,facts).join('2026-11-10T12:00:00Z')
        self.assertLessEqual(result['counts']['joined'],50)
        joined_ids=[row['fact_id'] for row in result['facts']]
        self.assertEqual(len(joined_ids),len(set(joined_ids)))
        self.assertEqual(len(result['facts']),50)
        for row in result['facts']:
            self.assertEqual(set(row['references']),{'vehicle','operator','software'})


class CliAssociationTests(unittest.TestCase):
    def _run(self,*arguments):
        return subprocess.run([sys.executable,'-m','datapack.cli',*arguments],
                              cwd=ROOT,capture_output=True,text=True)

    def test_join_writes_fact_file_and_error_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            joins=Path(directory)/'joins.jsonl'
            errors=Path(directory)/'errors.jsonl'
            result=self._run('join','samples','--cutoff','2026-11-10T12:00:00Z',
                             '--output',str(joins),'--errors',str(errors))
            self.assertEqual(result.returncode,0,result.stderr)
            summary=json.loads(result.stdout)
            self.assertEqual(summary['counts']['joined'],2)
            successful=[json.loads(line) for line in joins.read_text().splitlines()]
            self.assertEqual({row['fact_id'] for row in successful},{'f1','f2'})
            self.assertEqual(len(successful),2)
            side_channel=[json.loads(line) for line in errors.read_text().splitlines()]
            self.assertEqual({row['fact_id'] for row in side_channel},{'f3','f4'})

    def test_latest_mode_is_reported_as_one_fixed_cutoff(self):
        with tempfile.TemporaryDirectory() as directory:
            joins=Path(directory)/'joins.jsonl'
            errors=Path(directory)/'errors.jsonl'
            result=self._run('join','samples','--cutoff','latest',
                             '--output',str(joins),'--errors',str(errors))
            self.assertEqual(result.returncode,0,result.stderr)
            summary=json.loads(result.stdout)
            self.assertEqual(summary['knowledge_cutoff'],
                             {'mode':'latest','cutoff':'2026-11-09T00:00:00Z'})

    def test_impact_command_reports_exact_scope(self):
        result=self._run('impact','samples',
                         '--from-cutoff','2026-11-07T12:00:00Z',
                         '--to-cutoff','2026-11-10T12:00:00Z')
        self.assertEqual(result.returncode,0,result.stderr)
        report=json.loads(result.stdout)
        self.assertEqual({item['fact_id'] for item in report['changed']},{'f2','f4'})
        self.assertEqual(report['partitions'],['nov'])

    def test_ambiguous_cutoff_is_a_command_failure(self):
        result=self._run('join','samples','--cutoff','2026-11-10T12:00:00',
                         '--output','state/should-not-exist.jsonl')
        self.assertEqual(result.returncode,2)
        self.assertEqual(result.stdout,'')


if __name__=='__main__':
    unittest.main()
