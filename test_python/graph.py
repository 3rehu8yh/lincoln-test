"""
Graph representation for the linking of pubmed articles, scientific journals,
and clinical trials.

This representation is used to type the JSON file.  Note that this typing
is only a hint and is not enforced at runtime.  To enforce the proper control
of I/O boundaries (serialization and deserialization), it would be necessary
to implement runtime type checking with either _ad hoc_ validation code or
some schema language such as JSON Schema.  Note that this runtime type
validation would ideally be associated with some semantic validation to
enforce some business rules.

For this test, we limit ourselves to `mypy`-checked type validation.
"""

from typing import Sequence, Optional
from typing_extensions import TypedDict
from .pipeline_util import ISO8601Date

#: The graph is drug-centric: the root node contains a child per drug.
#: In turn, these nodes contain the references, grouped by reference kind
#: (articles, journals, clinical trials).
Graph = TypedDict('Graph', {'drugs': Sequence['Drug']})

Drug = TypedDict(
    'Drug',
    {
        #: The ID of the drug ("atccode").
        'id': str,
        #: The name of the drug, in upper case.
        'name': str,
        #: A list of clinical trials that mention this drug.
        'clinical_trial_references': Sequence['ClinicalTrialReference'],
        #: A list of articles that mention this drug.
        'article_references': Sequence['ArticleReference'],
        #: A list of journals that mention this drug.
        'journal_references': Sequence['JournalReference'],
    })

ArticleReference = TypedDict(
    'ArticleReference',
    {
        #: The date of publication.
        'date': ISO8601Date,
        #: The name of the article.
        'article_name': str,
    })

JournalReference = TypedDict(
    'JournalReference',
    {
        #: The date of publication, in this journal, of the PubMed articles or
        # clinical trials that mention the drug.
        'date': ISO8601Date,
        #: The name of the journal.
        'journal_name': str,
        #: The number of distinct publications (either PubMed articles
        #: or clinical trials) that mention the drug at this date and in
        #: this journal.
        'ref_count': int,
    })

ClinicalTrialReference = TypedDict(
    'ClinicalTrialReference',
    {
        #: The date of the publication for the clinical trial.
        'date': ISO8601Date,
        #: The name of the clinical trial.
        'clinical_trial_name': str,
    })
