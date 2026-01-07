using System;
using System.Text;

namespace GaldrDbSourceGenerators
{
    /// <summary>
    /// A string builder that automatically handles indentation for generated code.
    /// Provides a clean API for generating properly formatted C# source code.
    /// </summary>
    internal sealed class IndentedStringBuilder
    {
        private readonly StringBuilder _builder;
        private int _indentLevel;
        private const string IndentString = "    "; // 4 spaces per indent level

        public IndentedStringBuilder()
        {
            _builder = new StringBuilder();
            _indentLevel = 0;
        }

        /// <summary>
        /// Appends a line with proper indentation.
        /// </summary>
        public IndentedStringBuilder AppendLine(string line = "")
        {
            if (!string.IsNullOrEmpty(line))
            {
                // Add indentation
                for (int i = 0; i < _indentLevel; i++)
                {
                    _builder.Append(IndentString);
                }
                _builder.AppendLine(line);
            }
            else
            {
                // Empty line - no indentation
                _builder.AppendLine();
            }

            return this;
        }

        /// <summary>
        /// Appends multiple lines, each with proper indentation.
        /// </summary>
        public IndentedStringBuilder AppendLines(params string[] lines)
        {
            foreach (var line in lines)
            {
                AppendLine(line);
            }
            return this;
        }

        /// <summary>
        /// Creates a code block with opening brace, increased indentation, and automatic closing.
        /// Use with 'using' statement for automatic closure.
        /// </summary>
        /// <example>
        /// using (builder.Block("public class MyClass"))
        /// {
        ///     builder.AppendLine("public int Value { get; set; }");
        /// }
        /// // Automatically generates closing brace and decreases indentation
        /// </example>
        public BlockIndenter Block(string header, bool endWithSemiColon = false)
        {
            AppendLine(header);
            AppendLine("{");
            _indentLevel++;
            return new BlockIndenter(this, endWithSemiColon);
        }

        /// <summary>
        /// Increases indentation level temporarily.
        /// Use with 'using' statement for automatic restoration.
        /// </summary>
        public Indenter Indent()
        {
            _indentLevel++;
            return new Indenter(this);
        }

        /// <summary>
        /// Manually increases the indentation level.
        /// </summary>
        public void IncreaseIndent()
        {
            _indentLevel++;
        }

        /// <summary>
        /// Manually decreases the indentation level.
        /// </summary>
        public void DecreaseIndent()
        {
            if (_indentLevel > 0)
                _indentLevel--;
        }

        /// <summary>
        /// Returns the current indentation level.
        /// </summary>
        public int IndentLevel => _indentLevel;

        /// <summary>
        /// Converts the builder to a string.
        /// </summary>
        public override string ToString()
        {
            return _builder.ToString();
        }

        /// <summary>
        /// Clears all content from the builder.
        /// </summary>
        public void Clear()
        {
            _builder.Clear();
            _indentLevel = 0;
        }

        /// <summary>
        /// Gets the current length of the generated content.
        /// </summary>
        public int Length => _builder.Length;

        /// <summary>
        /// IDisposable wrapper for automatic block closure.
        /// </summary>
        public readonly struct BlockIndenter : IDisposable
        {
            private readonly IndentedStringBuilder _builder;
            private readonly bool _endWithSemiColon;

            internal BlockIndenter(IndentedStringBuilder builder, bool endWithSemiColon = false)
            {
                _builder = builder;
                _endWithSemiColon = endWithSemiColon;
            }

            public void Dispose()
            {
                _builder._indentLevel--;
                _builder.AppendLine(_endWithSemiColon ? "};" : "}");
            }
        }

        /// <summary>
        /// IDisposable wrapper for automatic indentation restoration.
        /// </summary>
        public readonly struct Indenter : IDisposable
        {
            private readonly IndentedStringBuilder _builder;

            internal Indenter(IndentedStringBuilder builder)
            {
                _builder = builder;
            }

            public void Dispose()
            {
                _builder._indentLevel--;
            }
        }
    }
}
