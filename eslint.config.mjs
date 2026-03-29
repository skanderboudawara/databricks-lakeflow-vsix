import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import importPlugin from 'eslint-plugin-import';
import jsdoc from 'eslint-plugin-jsdoc';

export default tseslint.config(
  // Global ignores
  { ignores: ['out/**', 'node_modules/**', 'media/**'] },

  // Base JS rules
  js.configs.recommended,

  // Extension host TypeScript (strict)
  {
    files: ['src/**/*.ts'],
    extends: [...tseslint.configs.recommended],
    plugins: { import: importPlugin, jsdoc },
    languageOptions: {
      parserOptions: {
        project: './tsconfig.json',
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
      '@typescript-eslint/consistent-type-imports': ['error', { prefer: 'type-imports' }],
      'import/order': [
        'warn',
        { groups: ['builtin', 'external', 'internal'], alphabetize: { order: 'asc' } },
      ],
      'import/no-duplicates': 'error',
      'eqeqeq': ['error', 'always'],
      'curly': ['error', 'all'],
      'no-console': 'off',

      // ── JSDoc enforcement ─────────────────────────────────────────────────
      // Every exported/public function, method, class, interface, and type
      // alias must have a JSDoc comment with a non-empty description.
      'jsdoc/require-jsdoc': [
        'error',
        {
          checkConstructors: false,
          require: {
            FunctionDeclaration: true,
            MethodDefinition: true,
            ClassDeclaration: true,
          },
          contexts: ['TSInterfaceDeclaration', 'TSTypeAliasDeclaration'],
        },
      ],
      'jsdoc/require-description': [
        'error',
        {
          contexts: [
            'FunctionDeclaration',
            'MethodDefinition',
            'ClassDeclaration',
            'TSInterfaceDeclaration',
            'TSTypeAliasDeclaration',
          ],
        },
      ],
      'jsdoc/check-alignment': 'warn',
      'jsdoc/check-syntax': 'warn',
    },
  },
);
