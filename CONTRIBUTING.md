# Contributing to smart-queue-nestjs

Thank you for your interest in contributing to smart-queue-nestjs!

## Production-Ready Status

This library is production-ready (v1.3.0+) with:
- Robust NestJS dependency injection (no undefined registry issues)
- Defensive error handling with clear messages
- Auto-registration of `ProcessorScannerService`
- Proper `BullBoardModule` integration

## Ways to Contribute

- **Bug Reports** - Report bugs by opening an issue
- **Feature Requests** - Suggest new features
- **Pull Requests** - Submit code improvements
- **Documentation** - Improve docs or add translations
- **Testing** - Write tests or report test coverage

## Development Setup

```bash
# Clone the repository
git clone https://github.com/nurulislamrimon/smart-queue-nestjs.git
cd smart-queue-nestjs

# Install dependencies
npm install

# Build the library
npm run build

# Run tests
npm test

# Run tests with coverage
npm run test:cov
```

## Code Style

- Use TypeScript with strict mode
- Follow existing code conventions
- Add types for all function parameters and return values
- Keep functions small and focused
- Ensure all providers use explicit DI (avoid bare class providers)

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run `npm run build` to ensure no build errors
5. Run `npm test` to ensure all tests pass
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## Commit Message Guidelines

- Use clear, descriptive commit messages
- Start with a verb (Add, Fix, Update, Remove)
- Example: `Add retry strategy support for workers`

## Issue Guidelines

- Search existing issues before creating new ones
- Use clear, descriptive titles
- Include reproduction steps for bugs
- Specify environment (Node.js, npm, OS versions)

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---
**Author:** Nurul Islam Rimon  
**Repository:** https://github.com/nurulislamrimon/smart-queue-nestjs
