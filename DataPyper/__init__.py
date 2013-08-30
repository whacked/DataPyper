import os
import numpy as np
import pandas as pd

from nipype.interfaces.base import TraitedSpec, BaseInterface, Bunch, isdefined
from nipype.interfaces.traits_extension import traits # , File
import nipype.pipeline.engine as pe


## TODO
# in the exec part pandas doesn't like columns with dots in them
# perhaps force rename header on read, or find out how to make it work


class CSVFileInputSpec(TraitedSpec):                                                                                                 
    csv_filepath = traits.File(mandatory = True, desc = "path to input csv file")
    rename_header = traits.Dict(desc = """
    dictionary of the form

    headername : newheadername
    """)

class CSVFileOutputSpec(TraitedSpec):
    """
    should be list of Bunch objects
    
    """
    data_frame = traits.Any()

class CSVFile(BaseInterface):

    input_spec = CSVFileInputSpec
    output_spec = CSVFileOutputSpec

    def _run_interface(self, runtime):
        runtime.returncode = 0
        return runtime

    def __init__(self, **inputs):
        super(CSVFile, self).__init__(**inputs)

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['data_frame'] = self._data_frame
        return outputs

    def _run_interface(self, runtime):
        self.inputs.rename_header = self.inputs.rename_header or {}
        cwd = os.getcwd()
        df = pd \
                .read_csv(self.inputs.csv_filepath) \
                .rename(columns = self.inputs.rename_header) \
                .sort('onset')

        ## derive a duration column if it doesn't exist
        if (df.columns == "duration").sum() == 0:
            df['duration'] = df['onset'].diff().shift(-1)

            ## FIXME
            ## stuff the last value with the mean duration
            ## there isn't any empirical reason why this is
            ## a good idea
            df['duration'][-1:] = df['duration'].mean()
        self._data_frame = df
        runtime.returncode = 0
        return runtime


class DataSelectorInputSpec(TraitedSpec):                                                                                                 
    data_frame             = traits.Any(desc = "input Pandas DataFrame object")
    condition_definition   = traits.List(desc = """\
                    a list of strings specifying the condition selection
                    methods. The selection uses Pandas syntax, in strings, that
                    gets eval()'ed at runtime.

                    Condition names corresponding to the variables passed to
                    the analysis program (e.g. FSL) are defined thus:

                        CONDITION_NAME  =  SELECTION_STRING

                    where CONDITION_NAME has no spaces; we split left and right
                    by the single equality sign ('='). Example input:

                        condition_definition = [
                            "remove!    = label.str.contains('INFO:wait_for_scanner')",
                            "chose_yes  = (runnum == {run_number}) & (response == 1)",
                        ]

                    `remove!` is a special directive that just drops all
                    matching rows.
            
            """)
    function_definition    = traits.Any()
    condition_value_feeder = traits.Dict(traits.Str, value = {}, usedefault = True,
            desc = """\
                    a "value feeder" that should be an input coming in from
                    e.g. an IdentityInterface node, like subject_id or
                    run_number;

                    this value gets used by template substitution prior
                    evaluation of the condition_definition strings.

                    Example:
                    select all rows with run number 1, you can do

                        (runnum == {run_number})

                    which implies your DataFrame has a colum called 'runnum',
                    and you will supply the actual value at run-time to the
                    input port called 'run_number'
            """)
    amplitude_definition   = traits.Any(desc = """\
                    `string` specifying the column name to be used as the
                    regressor height (note this gets ignored in SPM level 1)
                    per nipype doc

                    http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.algorithms.modelgen.html

                    Example:
                        
                        amplitude_definition = 'response_time'

                    ***OR***

                    `dictionary` that maps the condition's column name to the
                    desired regressor height's column name. this implies that
                    you should set up your data table's regressors prior
                    calling DataSelector. Anything that isn't mapped in the
                    dictionary will get assigned `None`, which in
                    CNELevel1Design will get a height of 1

                    Example:

                        amplitude_definition = {
                            'product': 'price',
                            'choice': 'response_time',
                        }

                    (Although arbitrary manipulation can be done through the
                    function_definition input, that really should be avoided)
            """)

    # copied from DataSink
    def __setattr__(self, key, value):
        if key not in self.copyable_trait_names():
            if not isdefined(value):
                super(DataSelectorInputSpec, self).__setattr__(key, value)
            self.condition_value_feeder[key] = value
        else:
            if key in self.condition_value_feeder:
                self.condition_value_feeder[key] = value
            super(DataSelectorInputSpec, self).__setattr__(key, value)

    def __getattr__(self, key):
        if key not in self.condition_value_feeder:
            if len(self.condition_value_feeder) is 0:
                self.__setattr__(key, traits.Any())
            else:
                return None
        return self.condition_value_feeder[key]

class DataSelectorOutputSpec(TraitedSpec):
    """
    should be list of Bunch objects
    
    """
    ## data_frame = traits.Any()
    subject_info = traits.Any()

class DataSelector(BaseInterface):

    input_spec = DataSelectorInputSpec
    output_spec = DataSelectorOutputSpec

    def _run_interface(self, runtime):
        runtime.returncode = 0
        return runtime

    def __init__(self, **inputs):
        super(DataSelector, self).__init__(**inputs)

    def _list_outputs(self):
        outputs = self.output_spec().get()
        ## outputs['data_frame'] = self._data_frame
        outputs['subject_info'] = self._subject_info
        return outputs

    def _k2var(self, k):
        # return "col_%s" % k
        return k

    def _run_interface(self, runtime):
        ENDTIME_COLNAME = 'xx_post_onset_termination'
        cwd = os.getcwd()
        df = self.inputs.data_frame

        self._dfn = {}
        if self.inputs.function_definition:
            if type(self.inputs.function_definition) is list:
                for fn in self.inputs.function_definition:
                    self._dfn[fn.__name__] = fn
            else:
                self._dfn = self.inputs.function_definition

        df[ENDTIME_COLNAME] = df['onset'] + df['duration']
        for k in df.keys():
            if k.startswith("Unnamed"): continue
            varname = self._k2var(k)
            exec("%s = df['%s']" % (varname, k))

        dcond = {}
        dcount = {}
        row_sum = 0
        for conddef in self.inputs.condition_definition:
            if type(conddef) is tuple:
                condname, condfn = conddef
                idx_match = condfn(df)
            else:
                spl_conddef = conddef.split('=', 1)

                # assume the condition is specified directly by a derivation function
                if len(spl_conddef) == 1:
                    condname = evalstr = spl_conddef[0]
                else:
                    condname = spl_conddef[0].strip()
                    evalstr = spl_conddef[1].strip().format(**self.inputs.condition_value_feeder)
                
                if evalstr in self._dfn:
                    idx_match = self._dfn[evalstr](df)
                else:
                    idx_match = eval(evalstr)

            if condname == "remove!":
                df = df[~idx_match]
            else:
                dcond[condname] = df[idx_match]
                dcount[conddef] = dcond[condname].shape[0]
                row_sum += dcond[condname].shape[0]
            del idx_match

        dfonset = pd.concat(dcond.values())

        ## FIXME
        ## make this more flexible
        ## essentially we want to sort from slow to fast
        ## on certain given columns
        ## i.e. run -> trial -> onset
        ## because a similar onset may appear over different runs
        lsksort = []
        if "run_number" in df.keys():
            lsksort.append("run_number")
        if "trial_number" in df.keys():
            lsksort.append("trial_number")
        lsksort.append("onset")

        ## FIXME:
        ## the overlap check tolerance below should be configurable
        ## check for time overlap (some other event happens during another event)
        ## naive test of whether any onset begins before the previous row's end time
        dfonset_sorted = dfonset.sort(lsksort)
        idx_is_overlap = dfonset_sorted.index[(
            ## next event's onset time + tolerance < this event's end time
            ((dfonset_sorted['onset'][1:] + 1.2) < dfonset_sorted[ENDTIME_COLNAME][:-1])
            ## the check only applies when the
            ## next event's onset time must also > this event's onset time
            & (dfonset_sorted['onset'][:-1] < dfonset_sorted['onset'][1:])
            )]
        if len(idx_is_overlap):
            sidx_is_overlap = idx_is_overlap.to_series()
            sidx_is_overlap = sidx_is_overlap.append(sidx_is_overlap - 1)
            sidx_is_overlap.sort()
            ## get the accused rows and the rows preceding those
            print("""
            * * * WARNING: * * *

            There are overlaps in your event specification!

            \n%s
            """ % (dfonset_sorted.ix[sidx_is_overlap]))

        ## check for duplicate onsets
        idx_duplicated_onset = dfonset.duplicated('onset')
        if idx_duplicated_onset.sum() > 0:
            v_duplicated_onset = dfonset[idx_duplicated_onset]['onset']
            col_display = [k for k in dfonset.keys() if k != ENDTIME_COLNAME]
            raise Exception("""
                    Overlapping events found!

                    These onsets are duplicated in your event specification:

                    \n%s
            
            """ % (dfonset[dfonset['onset'].isin(v_duplicated_onset)][col_display]))

        ## check for over-specified model
        if row_sum > df.shape[0]:
            raise Exception("""
                    There are more events in your specification
                    than there are total rows in the data!
                    You specified : %s
                    Total rows    : %s

                    The conditions you specified:
                    =============================
                    %s
                    """ % (row_sum, df.shape[0], str("\n"+(" " * 20)).join(["(%s) %s"%(str(count).rjust(3), conddef) for conddef,count in dcount.items()])))

        ## self._data_frame = self.inputs.data_frame
        condname_list = dcond.keys()
        if self.inputs.amplitude_definition:
            ## assume it's a string!
            if not type(self.inputs.amplitude_definition) is dict:
                mapfn = lambda _: self.inputs.amplitude_definition
            else:
                mapfn = self.inputs.amplitude_definition.get
        else:
            mapfn = lambda _: None
        ampmap = dict([(condname, mapfn(condname)) for condname in condname_list])

        self._subject_info = Bunch(
                conditions = condname_list,
                onsets     = [cond['onset'   ].tolist() for cond in dcond.values()],
                durations  = [cond['duration'].tolist() for cond in dcond.values()],
                amplitudes = [ampmap[condname] and cond[ampmap[condname]].tolist() or None for condname in condname_list],
                )

        runtime.returncode = 0
        return runtime


if __name__ == "__main__":
    

    from pandas import DataFrame as DF
    import sys

    ## this stuff should be moved to a unit test
    if "--test" in sys.argv:


        ## generate some random csv datastructure
        import faker, random, tempfile
        FK = faker.Faker()
        ncol = random.randint(1, 5)
        nrow = random.randint(100, 400)

        df = DF(dict([(key, np.random.rand(1, nrow)[0]) for key in [FK.username() for i in range(ncol-1)] + ["RT"]]))
        for k in df.keys():
            if k == "RT": continue
            if random.random() > 0.5:
                ## turn the column into a binary value
                df[k] = df[k].round()

        TR_duration = 0.5 * random.randint(2, 6)
        ## append a duration and onset
        df['duration'] = TR_duration
        df['onset'] = df.index * TR_duration

        csv_filepath = tempfile.mktemp(suffix = ".csv")
        df.to_csv(csv_filepath)

        csvf = pe.Node(name = "csvfile", interface = CSVFile())
        csvf.inputs.csv_filepath = csv_filepath
        csvf.inputs.rename_header = {
                'current_time': 'onset',
                
                }
        ## res = csvf.run()
        ds = pe.Node(name = "dataselector", interface = DataSelector())
        ds.inputs.condition_definition = [
                "run1 = onset >= 10",
                "run2 = (onset > 20) & (onset < 30)",
                
                ]

        wf = pe.Workflow(name="wf")
        wf.base_dir = "/tmp"
        wf.config['crashdump_dir'] = "/tmp/crashdump"
        wf.connect([
            (csvf, ds, [("data_frame", "data_frame")]),
            ])

        res = wf.run()

        os.unlink(csv_filepath)
    else:

        from pdloader import df

        def chose_no(df):
            return df['response'] != 1

        ds = DataSelector(
                data_frame = df,
                amplitude_column = 'height',
                condition_definition = [
                "remove! = label.str.contains('INFO:wait_for_scanner')",
                "PROD_run_1 = (run_number == 1) & (evname == 'PRODUCT')",
                "chose_yes = (run_number == {run_number}) & (evname == 'CHOICE') & (response == 1)",
                ## "chose_no", # or "chose_no = chose_no" or "whatever = chose_no"
                ## ("chose_no", chose_no), # or "chose_no = chose_no" or "whatever = chose_no"
            ])

        ds.inputs.function_definition = [
                chose_no,
                ]
        ## ds.inputs.condition_value_feeder = {
        ##         'run_number': 1,
        ##         
        ##         }
        ds.inputs.run_number = 1
        res = ds.run()
        print res.outputs

    #except Exception, e:
    #    raise e


