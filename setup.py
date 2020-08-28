from setuptools import setup, find_packages

setup(
    name="ocelot",
    version="0.1",
    description="Scrape and analyze 13f reports",
    packages=find_packages("src"),
    package_dir={"":"src"},
    entry_points={"console_scripts": ["ocelot = ocelot.__main__:main"]}
)