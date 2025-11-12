# Contributing

Thank you for your interest in contributing to the nanopore-transfer-automation tool! We welcome contributions from the community.

## How to Contribute

### Reporting Issues

- Check if the issue already exists in the [issue tracker](../../issues)
- If not, create a new issue with a clear title and description
- Include steps/command(s) to reproduce the problem if applicable, as well as any relevant error logs

### Making Changes

1. Fork the repository
2. Create a new branch for your feature or fix:
   ```bash
   git checkout -b your-feature-name
   ```
3. Make your changes
4. Create test data using the [provided script](../../blob/main/.test/make_test_data.py)
5. Run tests via [the test script](../../blob/main/test.sh) and make sure they pass
6. Commit your changes with clear commit message(s)
7. Push to your fork and submit a pull request

Note that if your feature introduces functionality not handled by the test data, you should also create test data to handle it via the script referenced in step 4.

### Pull Request Guidelines

- Keep pull requests focused on a single change
- Write clear, descriptive commit messages
- Include tests for new functionality when applicable
- Update documentation as needed
- Follow the existing code style and conventions

### Code Style

- Please lint your snakemake code using 
- Follow the existing code formatting and style
- Write clear, readable code with appropriate comments
- Ensure your code passes [snakefmt](https://pypi.org/project/snakefmt/) linting

## Questions?

If you have questions, feel free to open an issue or reach out to us.

Thank you for contributing! 🎉
