using System;

namespace GaldrDbEngine.IO;

/// <summary>
/// Exception thrown when the provided password is incorrect or when attempting to
/// open an encrypted database without providing a password.
/// </summary>
public sealed class InvalidPasswordException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidPasswordException"/> class with a default message.
    /// </summary>
    public InvalidPasswordException()
        : base("The provided password is incorrect or the database is encrypted and no password was provided.")
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidPasswordException"/> class with a specified message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public InvalidPasswordException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidPasswordException"/> class with a specified message
    /// and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public InvalidPasswordException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
