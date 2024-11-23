# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.coverage',
    'sphinx.ext.doctest',
    'sphinx.ext.extlinks',
    'sphinx.ext.ifconfig',
    'sphinx.ext.napoleon',
    'sphinx.ext.todo',
    'sphinx.ext.viewcode'
]
source_suffix = '.rst'
master_doc = 'index'
project = 'questdb'
year = '2024'
author = 'QuestDB'
copyright = '{0}, {1}'.format(year, author)
version = release = '2.0.3'

github_repo_url = 'https://github.com/questdb/py-questdb-client'

pygments_style = 'trac'
templates_path = ['.']
extlinks = {
    'issue': (f'{github_repo_url}/issues/%s', '#%s'),
    'pr': (f'{github_repo_url}/pull/%s', 'PR #%s'),
    'commit': (f"{github_repo_url}/commit/%s", "%s"),
}

# on_rtd is whether we are on readthedocs.org
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

if not on_rtd:  # only set the theme if we're building docs locally
    html_theme = 'alabaster'

html_use_smartypants = True
html_last_updated_fmt = '%b %d, %Y'
html_split_index = False
html_sidebars = {
    '**': [
        'about.html',
        'searchbox.html',
        'globaltoc.html',
        'sourcelink.html'
    ],
}
html_theme_options = {
    'description': 'Python client for QuestDB',
    'github_button': True,
    'github_user': 'questdb',
    'github_repo': 'py-questdb-client',
}

html_short_title = '%s-%s' % (project, version)

napoleon_use_ivar = True
napoleon_use_rtype = False
napoleon_use_param = False

autodoc_default_options = {
    'special-members': '__init__ , __str__ , __enter__ , __exit__',
    'undoc-members': True
}


# def do_not_skip_dunder_members(_app, _what, name, _obj, would_skip, _options):
#     if name in ('__init__', '__call__', '__str__', '__enter__', '__exit__'):
#         return False
#     return would_skip


# def setup(app):
#     app.connect('autodoc-skip-member', do_not_skip_dunder_members)