import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="demandprediction",
    version='0.1.0',
    install_requires=["pandas>=1.1.0","statsmodels","sklearn"],
    extras_require={
        "dev": ["pytest>=6.2.1"],
        "test": ["pytest>=6.2.1"],
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
