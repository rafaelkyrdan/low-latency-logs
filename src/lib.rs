//! Low latency logs
//!

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[test]
    fn test_addition() {
        assert_eq!(add(0, 0), 0);
        assert_eq!(add(1, 1), 2);
        assert_eq!(add(10, 5), 15);
    }
}
