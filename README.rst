 -- version-pub
python setup.py sdist bdist_wheel
twine upload dist/*

pip3 install kombuzstest --trusted-host pypi.sankuai.com -i https://pypi.org/simple
Found existing installation: kombu 4.6.11
.
