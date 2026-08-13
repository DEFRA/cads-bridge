using CadsBridge.Core.Domain.BusinessRules;
using CadsBridge.Core.Exceptions;
using FluentAssertions;
using Moq;

namespace CadsBridge.Core.Tests.Unit.Domain.BusinessRules;

public class BusinessRuleCheckerTests
{
    [Fact]
    public void ShouldReturnTrueWhenAllRulesAreValid()
    {
        // Arrange
        var validRule1 = new Mock<IBusinessRule>();
        validRule1.Setup(r => r.IsBroken()).Returns(false);
        validRule1.Setup(r => r.Message).Returns("Valid Rule 1");

        var validRule2 = new Mock<IBusinessRule>();
        validRule2.Setup(r => r.IsBroken()).Returns(false);
        validRule2.Setup(r => r.Message).Returns("Valid Rule 2");

        // Act
        Action act = () => BusinessRuleChecker.CheckRule(validRule1.Object, validRule2.Object);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void ShouldThrowFirstExceptionWhenAnyRuleIsBroken()
    {
        // Arrange
        var validRule = new Mock<IBusinessRule>();
        validRule.Setup(r => r.IsBroken()).Returns(false);
        validRule.Setup(r => r.Message).Returns("Valid Rule");

        var brokenRule1 = new Mock<IBusinessRule>();
        brokenRule1.Setup(r => r.IsBroken()).Returns(true);
        brokenRule1.Setup(r => r.Message).Returns("Broken Rule 1");

        var brokenRule2 = new Mock<IBusinessRule>();
        brokenRule2.Setup(r => r.IsBroken()).Returns(true);
        brokenRule2.Setup(r => r.Message).Returns("Broken Rule 2");

        // Act
        Action act = () => BusinessRuleChecker.CheckRule(validRule.Object, brokenRule1.Object, brokenRule2.Object);

        // Assert
        act.Should().Throw<BusinessRuleValidationException>().WithMessage("Broken Rule 1");
    }

    [Fact]
    public void ShouldIncludeBrokenRuleTypeAndMessageInToString()
    {
        // Arrange
        var validRule = new StubBusinessRule(isBroken: false, "Valid Rule");
        var brokenRule1 = new StubBusinessRule(isBroken: true, "Broken Rule 1");
        var brokenRule2 = new StubBusinessRule(isBroken: true, "Broken Rule 2");

        // Act
        Action act = () => BusinessRuleChecker.CheckRule(validRule, brokenRule1, brokenRule2);

        // Assert
        var exception = act.Should().Throw<BusinessRuleValidationException>().Which;

        exception.ToString().Should().Be($"{typeof(StubBusinessRule).FullName}: Broken Rule 1");
    }

    private sealed class StubBusinessRule(bool isBroken, string message) : IBusinessRule
    {
        public string Message { get; } = message;

        public bool IsBroken() => isBroken;
    }
}