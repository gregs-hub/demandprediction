import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="demandprediction",
    version='0.3.0',
    install_requires=[],
    extras_require={
        "dev": [],
        "test": [],
    },
    author="Gregory Seiller",
    author_email="grse@dhigroup.com",
    description="Water Demand Prediction",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DHIgrse/demandprediction",
    packages=setuptools.find_packages(),
    include_package_data=True,
)
